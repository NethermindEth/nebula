package discv5

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	ma "github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"

	"github.com/dennis-tra/nebula-crawler/config"
	"github.com/dennis-tra/nebula-crawler/core"
	"github.com/dennis-tra/nebula-crawler/db"
	pgmodels "github.com/dennis-tra/nebula-crawler/db/models/pg"
)

// AztecDiscV5OnlyCrawler implements a pure DiscV5-only crawler for Aztec networks
// that completely bypasses libp2p connections to avoid aggressive disconnection policies
type AztecDiscV5OnlyCrawler struct {
	id           string
	cfg          *AztecDiscV5OnlyConfig
	listener     *discover.UDPv5
	crawledPeers int
	done         chan struct{}
}

type AztecDiscV5OnlyConfig struct {
	Network          config.Network
	DialTimeout      time.Duration
	AddrDialType     config.AddrType
	KeepENR          bool
	LogErrors        bool
	MaxJitter        time.Duration
	Discv5ProtocolID [6]byte
	StrictValidation bool   // Enable strict Aztec peer validation
	MinAztecVersion  uint64 // Minimum required Aztec version
	RequiredChainID  uint64 // Required Aztec chain ID
}

var _ core.Worker[PeerInfo, core.CrawlResult[PeerInfo]] = (*AztecDiscV5OnlyCrawler)(nil)

func (c *AztecDiscV5OnlyCrawler) Work(ctx context.Context, task PeerInfo) (core.CrawlResult[PeerInfo], error) {
	// add a startup jitter delay to prevent all workers to crawl at exactly the
	// same time and potentially overwhelm the machine that Nebula is running on
	if c.crawledPeers == 0 {
		jitter := time.Duration(0)
		if c.cfg.MaxJitter > 0 { // could be <= 0 if the worker count is 1
			jitter = time.Duration(rand.Int63n(int64(c.cfg.MaxJitter)))
		}
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
		}
	}

	logEntry := log.WithFields(log.Fields{
		"crawlerID":  c.id,
		"remoteID":   task.peerID.ShortString(),
		"crawlCount": c.crawledPeers,
	})
	logEntry.Debugln("Crawling peer (DiscV5-only)")
	defer logEntry.Debugln("Crawled peer (DiscV5-only)")

	crawlStart := time.Now()

	// ONLY crawl via DiscV5 - no libp2p connections
	discV5ResultCh := c.crawlDiscV5Only(ctx, task)
	discV5Result := <-discV5ResultCh

	properties := c.PeerProperties(task.Node)

	// keep track of all unknown crawl errors
	if discV5Result.ErrorStr == pgmodels.NetErrorUnknown && discV5Result.Error != nil {
		properties["crawl_error"] = discV5Result.Error.Error()
	}

	// Mark this as DiscV5-only crawl
	properties["crawl_method"] = "discv5_only"
	properties["aztec_network"] = true

	data, err := json.Marshal(properties)
	if err != nil {
		log.WithError(err).WithField("properties", properties).Warnln("Could not marshal peer properties")
	}

	// Use only DiscV5 connection info
	connectMaddr := discV5Result.ConnectMaddr

	// For DiscV5-only, we don't have dial attempts or extra addresses
	// since we're not establishing libp2p connections
	var (
		dialMaddrs     []ma.Multiaddr
		filteredMaddrs []ma.Multiaddr
		extraMaddrs    []ma.Multiaddr
	)

	// All addresses are "filtered" since we didn't dial them
	filteredMaddrs = task.maddrs

	// Determine if peer is dialable based on DiscV5 results
	// A peer is considered undialable if:
	// 1. No response to ENR request (timeout)
	// 2. DiscV5 crawl errors (io_timeout, etc.)
	// 3. Missing required Aztec ENR entries
	var connectError error
	var connectErrorStr string

	if discV5Result.Error != nil {
		// If DiscV5 crawling failed, mark as undialable
		connectError = discV5Result.Error
		connectErrorStr = discV5Result.ErrorStr
	} else if discV5Result.RespondedAt == nil {
		// If no response to ENR request, mark as undialable
		connectError = fmt.Errorf("no response to ENR request")
		connectErrorStr = "no_response"
	} else {
		// Validate Aztec-specific requirements
		if !c.isValidAztecPeer(task.Node) {
			connectError = fmt.Errorf("invalid Aztec peer: missing required ENR entries")
			connectErrorStr = "invalid_aztec_peer"
		}
	}

	cr := core.CrawlResult[PeerInfo]{
		CrawlerID:           c.id,
		Info:                task,
		CrawlStartTime:      crawlStart,
		RoutingTableFromAPI: false,
		RoutingTable:        discV5Result.RoutingTable,
		Agent:               "",         // No agent info without libp2p connection
		Protocols:           []string{}, // No protocol info without libp2p connection
		DialMaddrs:          dialMaddrs,
		FilteredMaddrs:      filteredMaddrs,
		ExtraMaddrs:         extraMaddrs,
		ConnectMaddr:        connectMaddr,
		DialErrors:          []string{}, // No dial errors since we don't dial
		ConnectError:        connectError,
		ConnectErrorStr:     connectErrorStr,
		CrawlError:          discV5Result.Error,
		CrawlErrorStr:       discV5Result.ErrorStr,
		CrawlEndTime:        time.Now(),
		ConnectStartTime:    time.Time{}, // No connection start time
		ConnectEndTime:      time.Time{}, // No connection end time
		Properties:          data,
		LogErrors:           c.cfg.LogErrors,
	}

	// We've now crawled this peer, so increment
	c.crawledPeers++

	return cr, nil
}

func (c *AztecDiscV5OnlyCrawler) PeerProperties(node *enode.Node) map[string]any {
	properties := map[string]any{}

	if ip := node.IP(); ip != nil {
		properties["ip"] = ip.String()
	}

	properties["seq"] = node.Record().Seq()

	if node.UDP() != 0 {
		properties["udp"] = node.UDP()
	}

	if node.TCP() != 0 {
		properties["tcp"] = node.TCP()
	}

	// Check for QUIC endpoint
	if quicAddr, ok := node.QUICEndpoint(); ok {
		properties["quic_port"] = quicAddr.Port()
		properties["quic_addr"] = quicAddr.Addr().String()
	}

	// Parse Aztec-specific ENR entries if they exist
	var enrEntryAztec ENREntryAztec
	if err := node.Load(&enrEntryAztec); err == nil {
		properties["aztec_version"] = enrEntryAztec.Version
		properties["aztec_chain_id"] = enrEntryAztec.ChainID
	}

	if c.cfg.KeepENR {
		properties["enr"] = node.String()
	}

	return properties
}

type AztecDiscV5OnlyResult struct {
	// The time we received the first successful response
	RespondedAt *time.Time

	// The multi address via which we received a response
	ConnectMaddr ma.Multiaddr

	// The updated ethereum node record
	ENR *enode.Node

	// The neighbors of the crawled peer
	RoutingTable *core.RoutingTable[PeerInfo]

	// The time the draining of bucket entries was finished
	DoneAt time.Time

	// The combined error of crawling the peer's buckets
	Error error

	// The above error mapped to a known string
	ErrorStr string
}

func (c *AztecDiscV5OnlyCrawler) crawlDiscV5Only(ctx context.Context, pi PeerInfo) chan AztecDiscV5OnlyResult {
	resultCh := make(chan AztecDiscV5OnlyResult)

	go func() {
		defer close(resultCh)

		result := AztecDiscV5OnlyResult{}

		// all neighbors of pi. We're using a map to deduplicate.
		allNeighbors := map[string]PeerInfo{}

		// errorBits tracks at which CPL errors have occurred.
		// 0000 0000 0000 0000 - No error
		// 0000 0000 0000 0001 - An error has occurred at CPL 0
		// 1000 0000 0000 0001 - An error has occurred at CPL 0 and 15
		errorBits := uint32(0)

		timeouts := 0
		enr, err := c.listener.RequestENR(pi.Node)
		if err != nil {
			timeouts += 1
			result.ENR = pi.Node
		} else {
			result.ENR = enr
			now := time.Now()
			result.RespondedAt = &now
		}

		// loop through the buckets sequentially because discv5 is also doing that
		// internally, so we won't gain much by spawning multiple parallel go
		// routines here. Stop the process as soon as we have received a timeout and
		// don't let the following calls time out as well.
		for i := 0; i <= discover.NBuckets; i++ { // 17 is maximum
			var neighbors []*enode.Node
			neighbors, err = c.listener.FindNode(pi.Node, []uint{uint(discover.HashBits - i)})
			if err != nil {
				if err == discover.ErrTimeout {
					timeouts += 1
					if timeouts < MaxCrawlRetriesAfterTimeout {
						continue
					}
				}

				errorBits |= (1 << i)
				err = fmt.Errorf("getting closest peer with CPL %d: %w", i, err)
				break
			}
			timeouts = 0

			if result.RespondedAt == nil {
				now := time.Now()
				result.RespondedAt = &now
				result.ConnectMaddr = pi.UDPMaddr()
			}

			for _, n := range neighbors {
				npi, err := NewPeerInfo(n)
				if err != nil {
					log.WithError(err).Warnln("Failed parsing ethereum node neighbor")
					continue
				}
				allNeighbors[string(npi.peerID)] = npi
			}
		}

		result.DoneAt = time.Now()
		// if we have at least a successful result, don't record error
		if noSuccessfulRequest(err, errorBits) {
			result.Error = err
		}

		result.RoutingTable = &core.RoutingTable[PeerInfo]{
			PeerID:    pi.ID(),
			Neighbors: []PeerInfo{},
			ErrorBits: uint16(errorBits),
			Error:     result.Error,
		}

		for _, n := range allNeighbors {
			result.RoutingTable.Neighbors = append(result.RoutingTable.Neighbors, n)
		}

		// if there was a connection error, parse it to a known one
		if result.Error != nil {
			result.ErrorStr = db.NetError(result.Error)
		}

		// send the result back and close channel
		select {
		case resultCh <- result:
		case <-ctx.Done():
		}
	}()

	return resultCh
}

// ENREntryAztec represents Aztec-specific ENR entries
type ENREntryAztec struct {
	Version uint64
	ChainID uint64
}

func (e *ENREntryAztec) ENRKey() string {
	return "aztec"
}

// isValidAztecPeer validates that a peer meets Aztec-specific requirements
func (c *AztecDiscV5OnlyCrawler) isValidAztecPeer(node *enode.Node) bool {
	// If strict validation is disabled, only check basic requirements
	if !c.cfg.StrictValidation {
		// Basic validation: ensure peer has valid network addresses
		if node.IP() == nil || node.UDP() == 0 {
			log.WithField("peerID", node.ID().String()).Debugln("Peer missing required network addresses")
			return false
		}
		return true
	}

	// Strict validation: check for required Aztec ENR entry
	var enrEntryAztec ENREntryAztec
	if err := node.Load(&enrEntryAztec); err != nil {
		log.WithError(err).WithField("peerID", node.ID().String()).Debugln("Peer missing Aztec ENR entry")
		return false
	}

	// Validate Aztec version
	if c.cfg.MinAztecVersion > 0 && enrEntryAztec.Version < c.cfg.MinAztecVersion {
		log.WithFields(log.Fields{
			"peerID":     node.ID().String(),
			"version":    enrEntryAztec.Version,
			"minVersion": c.cfg.MinAztecVersion,
		}).Debugln("Peer has outdated Aztec version")
		return false
	}

	// Validate chain ID
	if c.cfg.RequiredChainID > 0 && enrEntryAztec.ChainID != c.cfg.RequiredChainID {
		log.WithFields(log.Fields{
			"peerID":          node.ID().String(),
			"chainID":         enrEntryAztec.ChainID,
			"requiredChainID": c.cfg.RequiredChainID,
		}).Debugln("Peer has incorrect Aztec chain ID")
		return false
	}

	// Additional validation: ensure peer has valid network addresses
	if node.IP() == nil || node.UDP() == 0 {
		log.WithField("peerID", node.ID().String()).Debugln("Peer missing required network addresses")
		return false
	}

	return true
}
