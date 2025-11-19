package behaviors

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
)

// StreamHandlerFunc is a function that creates a stream handler
type StreamHandlerFunc func(*basichost.BasicHost) func(s interface{})

// NetworkBehavior defines the interface for network-specific behaviors
type NetworkBehavior interface {
	IdentifyTimeout() time.Duration
	ProcessCrawlResult(connectErr error, connectErrStr string, discv5Responded bool, properties map[string]any, metadataResponse interface{}) (error, string)
	RequestMetadata(ctx context.Context, host *basichost.BasicHost, peerID peer.ID) (interface{}, error)
	StreamHandlers() map[protocol.ID]StreamHandlerFunc
	Name() string
}

// DefaultBehavior implements the standard network behavior used by most networks.
// It provides sensible defaults without any special handling.
type DefaultBehavior struct{}

var _ NetworkBehavior = (*DefaultBehavior)(nil)

// NewDefault creates a new default behavior instance
func NewDefault() *DefaultBehavior {
	return &DefaultBehavior{}
}

// IdentifyTimeout returns the standard 5 second timeout for identify exchange
func (d *DefaultBehavior) IdentifyTimeout() time.Duration {
	return 5 * time.Second
}

// ProcessCrawlResult performs no special processing - just returns the errors as-is
func (d *DefaultBehavior) ProcessCrawlResult(
	connectErr error,
	connectErrStr string,
	discv5Responded bool,
	properties map[string]any,
	metadataResponse interface{},
) (error, string) {
	// No special processing - return errors unchanged
	return connectErr, connectErrStr
}

// RequestMetadata does not perform any metadata requests for default networks
func (d *DefaultBehavior) RequestMetadata(ctx context.Context, host *basichost.BasicHost, peerID peer.ID) (interface{}, error) {
	return nil, nil
}

// StreamHandlers returns no custom stream handlers for default networks
func (d *DefaultBehavior) StreamHandlers() map[protocol.ID]StreamHandlerFunc {
	return nil
}

// Name returns the name of this behavior
func (d *DefaultBehavior) Name() string {
	return "Default"
}

