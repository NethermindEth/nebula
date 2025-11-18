package behaviors

import (
	"context"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
)

// EthereumConsensusBehavior implements behavior for Ethereum Consensus Layer networks.
// This includes Ethereum Mainnet, Holesky, and other consensus networks.
// These networks require specific protocol handlers to prevent disconnections.
type EthereumConsensusBehavior struct{}

var _ NetworkBehavior = (*EthereumConsensusBehavior)(nil)

// NewEthereumConsensus creates a new Ethereum Consensus behavior instance
func NewEthereumConsensus() *EthereumConsensusBehavior {
	return &EthereumConsensusBehavior{}
}

// IdentifyTimeout returns the standard 5 second timeout
func (e *EthereumConsensusBehavior) IdentifyTimeout() time.Duration {
	return 5 * time.Second
}

// ProcessCrawlResult performs no special processing - just returns the errors as-is
func (e *EthereumConsensusBehavior) ProcessCrawlResult(
	connectErr error,
	connectErrStr string,
	discv5Responded bool,
	properties map[string]any,
	metadataResponse interface{},
) (error, string) {
	return connectErr, connectErrStr
}

// RequestMetadata does not perform any metadata requests
func (e *EthereumConsensusBehavior) RequestMetadata(ctx context.Context, host *basichost.BasicHost, peerID peer.ID) (interface{}, error) {
	return nil, nil
}

// StreamHandlers returns Ethereum consensus protocol handlers
// According to Diva, these are required protocols. Some of them are just
// assumed to be required. We just read from the stream indefinitely to
// gain time for the identify exchange to finish. We just pretend to support
// these protocols and keep the stream busy until we have gathered all the
// information we were interested in. This includes the agent version and
// all supported protocols.
func (e *EthereumConsensusBehavior) StreamHandlers() map[protocol.ID]StreamHandlerFunc {
	return map[protocol.ID]StreamHandlerFunc{
		"/eth2/beacon_chain/req/ping/1/ssz_snappy": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
		"/eth2/beacon_chain/req/status/1/ssz_snappy": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
		"/eth2/beacon_chain/req/metadata/1/ssz_snappy": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
		"/eth2/beacon_chain/req/metadata/2/ssz_snappy": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
		"/eth2/beacon_chain/req/goodbye/1/ssz_snappy": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
		"/meshsub/1.1.0": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
	}
}

func (e *EthereumConsensusBehavior) Name() string {
	return "EthereumConsensus"
}
