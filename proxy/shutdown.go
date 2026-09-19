// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrDraining is the error returned when a new request is rejected because
// the gateway is draining (shutting down gracefully)
var ErrDraining = errors.New("gateway is draining: new backend requests are not accepted")

// draining flags whether the proxy layer is draining. While draining, the
// proxies built by the default factory reject new backend requests but let
// the in-flight ones complete.
var draining atomic.Bool

// StartDraining flags the proxy layer as draining. It is called by the http
// server when the graceful shutdown starts.
func StartDraining() {
	draining.Store(true)
}

// IsDraining reports whether the proxy layer is draining.
func IsDraining() bool {
	return draining.Load()
}

// NewDrainMiddleware returns a middleware that rejects new requests with
// ErrDraining once the gateway starts draining. Requests already in flight
// are allowed to complete.
func NewDrainMiddleware(next Proxy) Proxy {
	return func(ctx context.Context, request *Request) (*Response, error) {
		if IsDraining() {
			return nil, ErrDraining
		}
		return next(ctx, request)
	}
}
