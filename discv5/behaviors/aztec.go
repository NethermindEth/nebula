package behaviors

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
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
type AztecBehavior struct{}

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

	// DEBUG: Log the processing
	log.WithFields(log.Fields{
		"discv5Responded": discv5Responded,
		"hadConnectErr":   connectErr != nil,
		"connectErrStr":   connectErrStr,
	}).Debugln("[AZTEC] ProcessCrawlResult called")

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
			log.Debugln("[AZTEC] Clearing libp2p error for discv5-responsive node")
		}

		// Clear the connection errors - node is considered online
		return nil, ""
	}

	// If node didn't respond to discv5, keep the original errors
	log.Debugln("[AZTEC] Node did not respond to discv5, keeping errors")
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

	// Set up handler to receive status from validator
	host.SetStreamHandler(protocol.ID("/aztec/req/status/1.0.0"), func(s network.Stream) {
		defer s.Close()
		log.WithFields(log.Fields{
			"remoteID": s.Conn().RemotePeer().ShortString(),
			"localID":  s.Conn().LocalPeer().ShortString(),
		}).Debugln("Received Aztec status stream from validator")

		data, err := io.ReadAll(s)
		if err != nil {
			log.WithError(err).WithField("remoteID", s.Conn().RemotePeer().ShortString()).Errorln("Error reading status data")
			return
		}

		log.WithFields(log.Fields{
			"remoteID": s.Conn().RemotePeer().ShortString(),
			"dataSize": len(data),
		}).Debugln("Successfully read status data")

		// Send data to main goroutine
		select {
		case statusChan <- data:
		default:
			log.WithField("remoteID", s.Conn().RemotePeer().ShortString()).Warnln("Status channel full, dropping data")
		}
	})

	// Add to peerstore so validator can find us
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
		// Successfully received status data
		encodedData := base64.RawStdEncoding.EncodeToString(data)
		log.WithFields(log.Fields{
			"remoteID":    pi.ShortString(),
			"dataSize":    len(data),
			"encodedSize": len(encodedData),
		}).Debugln("Successfully received and encoded status data")
		return encodedData, nil
	}
}

// StreamHandlers returns the Aztec protocol handlers
func (a *AztecBehavior) StreamHandlers() map[protocol.ID]StreamHandlerFunc {
	return map[protocol.ID]StreamHandlerFunc{
		"/aztec/req/status/1.0.0": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
		"/aztec/req/ping/1.0.0": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
		"/aztec/id/1.0.0": func(h *basichost.BasicHost) func(s interface{}) {
			return func(s interface{}) {
				if stream, ok := s.(network.Stream); ok {
					io.ReadAll(stream)
				}
			}
		},
	}
}

// Name returns the name of this behavior
func (a *AztecBehavior) Name() string {
	return "Aztec"
}
