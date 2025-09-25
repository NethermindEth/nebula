package discv5

import (
	"crypto/ecdsa"
	crand "crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/dennis-tra/nebula-crawler/config"
	"github.com/dennis-tra/nebula-crawler/core"
	"github.com/dennis-tra/nebula-crawler/db"
	"github.com/dennis-tra/nebula-crawler/utils"
)

type AztecDiscV5OnlyDriverConfig struct {
	Version          string
	Network          config.Network
	DialTimeout      time.Duration
	BootstrapPeers   []*enode.Node
	CrawlWorkerCount int
	AddrDialType     config.AddrType
	AddrTrackType    config.AddrType
	KeepENR          bool
	MeterProvider    metric.MeterProvider
	TracerProvider   trace.TracerProvider
	LogErrors        bool
	UDPBufferSize    int
	UDPRespTimeout   time.Duration
	Discv5ProtocolID [6]byte
	StrictValidation bool   // Enable strict Aztec peer validation
	MinAztecVersion  uint64 // Minimum required Aztec version
	RequiredChainID  uint64 // Required Aztec chain ID
}

func (cfg *AztecDiscV5OnlyDriverConfig) CrawlerConfig() *AztecDiscV5OnlyConfig {
	return &AztecDiscV5OnlyConfig{
		Network:          cfg.Network,
		DialTimeout:      cfg.DialTimeout,
		AddrDialType:     cfg.AddrDialType,
		KeepENR:          cfg.KeepENR,
		LogErrors:        cfg.LogErrors,
		MaxJitter:        time.Duration(cfg.CrawlWorkerCount/50) * time.Second, // e.g., 3000 workers evenly distributed over 60s
		Discv5ProtocolID: cfg.Discv5ProtocolID,
		StrictValidation: cfg.StrictValidation,
		MinAztecVersion:  cfg.MinAztecVersion,
		RequiredChainID:  cfg.RequiredChainID,
	}
}

func (cfg *AztecDiscV5OnlyDriverConfig) WriterConfig() *core.CrawlWriterConfig {
	return &core.CrawlWriterConfig{
		AddrTrackType: cfg.AddrTrackType,
	}
}

type AztecDiscV5OnlyDriver struct {
	cfg          *AztecDiscV5OnlyDriverConfig
	dbc          db.Client
	tasksChan    chan PeerInfo
	peerstore    *enode.DB
	crawlerCount int
	writerCount  int
	crawler      []*AztecDiscV5OnlyCrawler
}

var _ core.Driver[PeerInfo, core.CrawlResult[PeerInfo]] = (*AztecDiscV5OnlyDriver)(nil)

func NewAztecDiscV5OnlyDriver(dbc db.Client, cfg *AztecDiscV5OnlyDriverConfig) (*AztecDiscV5OnlyDriver, error) {
	tasksChan := make(chan PeerInfo, len(cfg.BootstrapPeers))
	for _, node := range cfg.BootstrapPeers {
		pi, err := NewPeerInfo(node)
		if err != nil {
			return nil, fmt.Errorf("new peer info from enr: %w", err)
		}
		tasksChan <- pi
	}
	close(tasksChan)

	peerstore, err := enode.OpenDB("") // in memory db
	if err != nil {
		return nil, fmt.Errorf("open in-memory peerstore: %w", err)
	}

	// set the discovery response timeout
	discover.RespTimeoutV5 = cfg.UDPRespTimeout

	return &AztecDiscV5OnlyDriver{
		cfg:       cfg,
		dbc:       dbc,
		tasksChan: tasksChan,
		peerstore: peerstore,
		crawler:   make([]*AztecDiscV5OnlyCrawler, 0),
	}, nil
}

// NewWorker is called multiple times but only log the configured buffer sizes once
var aztecLogOnce sync.Once

func (d *AztecDiscV5OnlyDriver) NewWorker() (core.Worker[PeerInfo, core.CrawlResult[PeerInfo]], error) {
	// If I'm not using the below elliptic curve, some Ethereum clients will reject communication
	priv, err := ecdsa.GenerateKey(ethcrypto.S256(), crand.Reader)
	if err != nil {
		return nil, fmt.Errorf("new ethereum ecdsa key: %w", err)
	}

	laddr := &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 0,
	}

	conn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return nil, fmt.Errorf("listen on udp4 port: %w", err)
	}

	if err = conn.SetReadBuffer(d.cfg.UDPBufferSize); err != nil {
		log.Warnln("Failed to set read buffer size on UDP listener", err)
	}

	rcvbuf, sndbuf, err := utils.GetUDPBufferSize(conn)
	aztecLogOnce.Do(func() {
		logEntry := log.WithFields(log.Fields{
			"rcvbuf": rcvbuf,
			"sndbuf": sndbuf,
			"rcvtgt": d.cfg.UDPBufferSize, // receive target
		})
		if rcvbuf < d.cfg.UDPBufferSize {
			logEntry.Warnln("Failed to increase UDP buffer sizes, using default")
		} else {
			logEntry.Infoln("Configured UDP buffer sizes")
		}
	})

	ethNode := enode.NewLocalNode(d.peerstore, priv)
	cfg := discover.Config{
		PrivateKey:   priv,
		ValidSchemes: enode.ValidSchemes,
		V5ProtocolID: &d.cfg.Discv5ProtocolID,
	}
	listener, err := discover.ListenV5(conn, ethNode, cfg)
	if err != nil {
		return nil, fmt.Errorf("listen discv5: %w", err)
	}

	c := &AztecDiscV5OnlyCrawler{
		id:       fmt.Sprintf("aztec-discv5-only-crawler-%02d", d.crawlerCount),
		cfg:      d.cfg.CrawlerConfig(),
		listener: listener,
		done:     make(chan struct{}),
	}

	d.crawlerCount += 1

	d.crawler = append(d.crawler, c)

	log.WithFields(log.Fields{
		"addr": conn.LocalAddr().String(),
	}).Debugln("Started Aztec DiscV5-only crawler worker", c.id)

	return c, nil
}

func (d *AztecDiscV5OnlyDriver) NewWriter() (core.Worker[core.CrawlResult[PeerInfo], core.WriteResult], error) {
	w := core.NewCrawlWriter[PeerInfo](fmt.Sprintf("writer-%02d", d.writerCount), d.dbc, d.cfg.WriterConfig())
	d.writerCount += 1
	return w, nil
}

func (d *AztecDiscV5OnlyDriver) Tasks() <-chan PeerInfo {
	return d.tasksChan
}

func (d *AztecDiscV5OnlyDriver) Close() {
	for _, c := range d.crawler {
		c.listener.Close()
	}
}
