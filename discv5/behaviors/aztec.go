package behaviors

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	log "github.com/sirupsen/logrus"

	pgmodels "github.com/dennis-tra/nebula-crawler/db/models/pg"
)

// AztecBehavior implements the Aztec network-specific behavior.
// Aztec uses a hybrid crawling approach where nodes that respond to discv5
// are considered online even if libp2p connection fails.
//
// The pendingRequests map routes incoming status stream responses to the correct
// waiting crawler. This is necessary because multiple crawlers share the same
// libp2p host, so a single global stream handler must demultiplex responses
// by remote peer ID.
type AztecBehavior struct {
	// pendingRequests maps peer.ID -> chan []byte for routing status responses
	pendingRequests sync.Map
}

var _ NetworkBehavior = (*AztecBehavior)(nil)

// NewAztec creates a new Aztec behavior instance
func NewAztec() *AztecBehavior {
	return &AztecBehavior{}
}

// IdentifyTimeout returns 60 seconds to accommodate Aztec's status request retries
func (a *AztecBehavior) IdentifyTimeout() time.Duration {
	return 60 * time.Second
}

// ProcessCrawlResult implements the hybrid online detection for Aztec:
// If a node responds to discv5, it's considered online even if libp2p fails.
// The libp2p error is preserved in separate properties for tracking.
func (a *AztecBehavior) ProcessCrawlResult(
	connectErr error,
	connectErrStr string,
	discv5Responded bool,
	properties map[string]any,
	metadataResponse interface{},
) (error, string) {
	// Track whether the node responded to discv5
	properties["aztec_discv5_online"] = discv5Responded

	// Add Aztec status response to properties if available
	if statusResponse, ok := metadataResponse.(string); ok && statusResponse != "" {
		properties["aztec_status_response"] = statusResponse
	}

	// If the node responded to discv5, treat it as online
	if discv5Responded {
		// Save the libp2p error for tracking purposes
		if connectErr != nil {
			properties["aztec_libp2p_error"] = connectErrStr
			if connectErrStr == pgmodels.NetErrorUnknown {
				properties["aztec_libp2p_error_detail"] = connectErr.Error()
			}
		}

		// Clear the connection errors - node is considered online
		return nil, ""
	}

	// If node didn't respond to discv5, keep the original errors
	return connectErr, connectErrStr
}

// RequestMetadata performs the Aztec status request with retries
func (a *AztecBehavior) RequestMetadata(ctx context.Context, host *basichost.BasicHost, peerID peer.ID) (interface{}, error) {
	const maxRetries = 5
	const retryInterval = 5 * time.Second

	log.WithFields(log.Fields{
		"remoteID":   peerID.ShortString(),
		"maxRetries": maxRetries,
		"interval":   retryInterval.String(),
	}).Debugln("Starting Aztec status request with retries")

	for attempt := 0; attempt < maxRetries; attempt++ {
		log.WithFields(log.Fields{
			"remoteID": peerID.ShortString(),
			"attempt":  attempt + 1,
			"of":       maxRetries,
		}).Debugln("Attempting Aztec status request")

		// Create a timeout context for this attempt
		attemptCtx, cancel := context.WithTimeout(ctx, retryInterval)
		status, err := a.attemptAztecStatus(attemptCtx, host, peerID)
		cancel()

		if err == nil {
			log.WithFields(log.Fields{
				"remoteID": peerID.ShortString(),
				"attempt":  attempt + 1,
			}).Infoln("Successfully received Aztec status")
			return status, nil
		}

		// Log retry attempt
		log.WithError(err).WithFields(log.Fields{
			"remoteID":   peerID.ShortString(),
			"attempt":    attempt + 1,
			"maxRetries": maxRetries,
		}).Warnln("Aztec status request failed")

		// Wait before retry (except on last attempt)
		if attempt < maxRetries-1 {
			log.WithFields(log.Fields{
				"remoteID": peerID.ShortString(),
				"waitTime": retryInterval.String(),
			}).Debugln("Waiting before retry")

			select {
			case <-ctx.Done():
				log.WithFields(log.Fields{
					"remoteID": peerID.ShortString(),
					"attempt":  attempt + 1,
				}).Warnln("Aztec status request cancelled")
				return "", ctx.Err()
			case <-time.After(retryInterval):
				continue
			}
		}
	}

	log.WithFields(log.Fields{
		"remoteID": peerID.ShortString(),
		"attempts": maxRetries,
	}).Errorln("Failed to get Aztec status after all retry attempts")

	return "", fmt.Errorf("failed to get Aztec status after %d attempts", maxRetries)
}

func (a *AztecBehavior) attemptAztecStatus(ctx context.Context, host *basichost.BasicHost, pi peer.ID) (string, error) {
	statusChan := make(chan []byte, 1)

	// Register this request so the stream handler (set up via StreamHandlers)
	// can route the response from this peer to our channel.
	a.pendingRequests.Store(pi, statusChan)
	defer a.pendingRequests.Delete(pi)

	addrInfo := host.Peerstore().PeerInfo(pi)

	log.WithFields(log.Fields{
		"remoteID": pi.ShortString(),
		"addrs":    len(addrInfo.Addrs),
	}).Debugln("Connecting to peer for status request")

	if err := host.Connect(ctx, addrInfo); err != nil {
		log.WithError(err).WithField("remoteID", pi.ShortString()).Debugln("Failed to connect to peer")
		return "", fmt.Errorf("connect to peer %s: %w", pi, err)
	}

	log.WithField("remoteID", pi.ShortString()).Debugln("Connected, waiting for status response")

	select {
	case <-ctx.Done():
		log.WithField("remoteID", pi.ShortString()).Debugln("Status request timed out")
		return "", ctx.Err()
	case data := <-statusChan:
		encodedData := base64.RawStdEncoding.EncodeToString(data)
		log.WithFields(log.Fields{
			"remoteID":    pi.ShortString(),
			"dataSize":    len(data),
			"encodedSize": len(encodedData),
		}).Debugln("Successfully received and encoded status data")
		return encodedData, nil
	}
}

// StreamHandlers returns the Aztec protocol handlers.
// The status handler routes incoming responses to the correct waiting crawler
// via the pendingRequests map, keyed by remote peer ID.
func (a *AztecBehavior) StreamHandlers() map[protocol.ID]StreamHandlerFunc {
	return map[protocol.ID]StreamHandlerFunc{
		"/aztec/req/status/1.0.0": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				stream, ok := s.(network.Stream)
				if !ok {
					return
				}
				defer stream.Close()

				remotePeer := stream.Conn().RemotePeer()

				data, err := io.ReadAll(io.LimitReader(stream, 1<<20)) // 1 MiB limit
				if err != nil {
					log.WithError(err).WithField("remoteID", remotePeer.ShortString()).Debugln("Error reading Aztec status data")
					return
				}

				if ch, ok := a.pendingRequests.Load(remotePeer); ok {
					select {
					case ch.(chan []byte) <- data:
					default:
						log.WithField("remoteID", remotePeer.ShortString()).Debugln("Status channel full, dropping data")
					}
				}
			}
		},
		"/aztec/req/ping/1.0.0": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					defer stream.Close()
					io.ReadAll(io.LimitReader(stream, 1<<20))
				}
			}
		},
		"/aztec/id/1.0.0": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					defer stream.Close()
					io.ReadAll(io.LimitReader(stream, 1<<20))
				}
			}
		},
	}
}

// Name returns the name of this behavior
func (a *AztecBehavior) Name() string {
	return "Aztec"
}
