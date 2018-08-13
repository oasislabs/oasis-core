// Package service provides service primitives.
package service

import (
	"github.com/oasislabs/ekiden/go/common/logging"
)

// CleanupAble provides a Cleanup method.
type CleanupAble interface {
	// Cleanup performs the service specific post-termination cleanup.
	Cleanup()
}

// BackgroundService is a background service.
type BackgroundService interface {
	// Name returns the service name.
	Name() string

	// Start starts the service.
	Start() error

	// Stop halts the service.
	Stop()

	// Quit returns a channel that will be closed when the service terminates.
	Quit() <-chan struct{}

	CleanupAble
}

// BaseBackgroundService is a base implementation of BackgroundService.
type BaseBackgroundService struct {
	name        string
	quitChannel chan struct{}
	Logger      *logging.Logger
}

// Name returns the service name.
func (b *BaseBackgroundService) Name() string {
	return b.name
}

// Start starts the service.
func (b *BaseBackgroundService) Start() error {
	return nil
}

// Stop halts the service.
func (b *BaseBackgroundService) Stop() {
	close(b.quitChannel)
}

// Quit returns a channel that will be closed when the service terminates.
func (b *BaseBackgroundService) Quit() <-chan struct{} {
	return b.quitChannel
}

// Cleanup performs the service specific post-termination cleanup.
func (b *BaseBackgroundService) Cleanup() {
	// Default implementation does nothing.
}

// NewBaseBackgroundService creates a new base background service implementation.
func NewBaseBackgroundService(name string) *BaseBackgroundService {
	return &BaseBackgroundService{
		name:        name,
		quitChannel: make(chan struct{}),
		Logger:      logging.GetLogger(name),
	}
}

type cleanupOnlyService struct {
	BaseBackgroundService

	svc CleanupAble
}

func (s *cleanupOnlyService) Quit() <-chan struct{} {
	panic("BUG: CleanupOnlyService does not implement Quit()")
}

func (s *cleanupOnlyService) Cleanup() {
	s.svc.Cleanup()
}

// NewCleanupOnlyService wraps a service as a cleanup only service.
func NewCleanupOnlyService(svc CleanupAble) BackgroundService {
	return &cleanupOnlyService{
		BaseBackgroundService: *NewBaseBackgroundService("<unknown>"),
		svc: svc,
	}
}
