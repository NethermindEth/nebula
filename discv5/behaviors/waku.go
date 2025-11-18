package behaviors

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	"github.com/libp2p/go-msgio/pbio"
	log "github.com/sirupsen/logrus"
	wakupb "github.com/waku-org/go-waku/waku/v2/protocol/metadata/pb"
)

// WakuMetadataResponse holds the cluster ID and shards returned from Waku metadata request
type WakuMetadataResponse struct {
	ClusterID uint32
	Shards    []uint32
}

// WakuBehavior implements the Waku network-specific behavior.
// Waku requires requesting metadata about cluster ID and shards.
type WakuBehavior struct {
	clusterID uint32
	shards    []uint32
}

var _ NetworkBehavior = (*WakuBehavior)(nil)

// NewWaku creates a new Waku behavior instance
func NewWaku(clusterID uint32, shards []uint32) *WakuBehavior {
	return &WakuBehavior{
		clusterID: clusterID,
		shards:    shards,
	}
}

// IdentifyTimeout returns the standard 5 second timeout
func (w *WakuBehavior) IdentifyTimeout() time.Duration {
	return 5 * time.Second
}

// ProcessCrawlResult extracts Waku metadata and adds it to properties
func (w *WakuBehavior) ProcessCrawlResult(
	connectErr error,
	connectErrStr string,
	discv5Responded bool,
	properties map[string]any,
	metadataResponse interface{},
) (error, string) {
	// Extract Waku metadata if available
	if wakuMeta, ok := metadataResponse.(*WakuMetadataResponse); ok {
		if wakuMeta.ClusterID != 0 && len(wakuMeta.Shards) > 0 {
			properties["waku_cluster_id"] = wakuMeta.ClusterID
			properties["waku_cluster_shards"] = wakuMeta.Shards
		}
	}

	// No special error handling - return errors unchanged
	return connectErr, connectErrStr
}

// RequestMetadata performs the Waku metadata request
func (w *WakuBehavior) RequestMetadata(ctx context.Context, host *basichost.BasicHost, peerID peer.ID) (interface{}, error) {
	// cannot import github.com/waku-org/go-waku/waku/v2/protocol/metadata
	// and use metadata.MetadataID_v1 because this would result in importing
	// incompatible packages

	s, err := host.NewStream(ctx, peerID, "/vac/waku/metadata/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &wakupb.WakuMetadataRequest{
		ClusterId: &w.clusterID,
		Shards:    w.shards,
	}

	writer := pbio.NewDelimitedWriter(s)
	reader := pbio.NewDelimitedReader(s, 4*1024*1024) // 4 MiB max

	if err = writer.WriteMsg(req); err != nil {
		_ = s.Reset()
		return nil, fmt.Errorf("write waku metadata request: %w", err)
	}

	response := &wakupb.WakuMetadataResponse{}
	if err = reader.ReadMsg(response); err != nil {
		_ = s.Reset()
		return nil, fmt.Errorf("read waku metadata response: %w", err)
	}

	log.WithFields(log.Fields{
		"remoteID":  peerID.ShortString(),
		"clusterID": response.GetClusterId(),
		"shards":    response.GetShards(),
	}).Debugln("Received Waku metadata")

	return &WakuMetadataResponse{
		ClusterID: response.GetClusterId(),
		Shards:    response.GetShards(),
	}, nil
}

// StreamHandlers returns no custom stream handlers for Waku
func (w *WakuBehavior) StreamHandlers() map[protocol.ID]StreamHandlerFunc {
	return nil
}

// Name returns the name of this behavior
func (w *WakuBehavior) Name() string {
	return "Waku"
}


