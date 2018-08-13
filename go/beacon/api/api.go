// Package api implements the random beacon API.
package api

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/oasislabs/ekiden/go/common/pubsub"
	epochtime "github.com/oasislabs/ekiden/go/epochtime/api"
)

// ErrBeaconNotAvailable is the error returned when a beacon is not
// available for the requested epoch for any reason.
var ErrBeaconNotAvailable = errors.New("beacon: random beacon not available")

// BeaconSize is the size of the beacon in bytes.
const BeaconSize = 32

// Backend is a random beacon implementation.
type Backend interface {
	// GetBeacon gets the beacon for the provided epoch.
	GetBeacon(context.Context, epochtime.EpochTime) ([]byte, error)

	// WatchBeacons returns a channel that produces a stream of
	// GenerateEvent.  Upon subscription, the most recently generate
	// beacon will be sent immediately if available.
	WatchBeacons() (<-chan *GenerateEvent, *pubsub.Subscription)
}

// GenerateEvent is the event that is returned via WatchBeacons to
// signify beacon generation.
type GenerateEvent struct {
	Epoch  epochtime.EpochTime
	Beacon []byte
}
