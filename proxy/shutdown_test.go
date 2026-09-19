// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
)

func TestDrainMiddleware_notDraining(t *testing.T) {
	draining.Store(false)

	calls := 0
	p := NewDrainMiddleware(func(_ context.Context, _ *Request) (*Response, error) {
		calls++
		return &Response{IsComplete: true}, nil
	})

	resp, err := p(context.Background(), &Request{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp == nil || !resp.IsComplete {
		t.Errorf("unexpected response: %v", resp)
	}
	if calls != 1 {
		t.Errorf("unexpected number of calls to the next proxy: %d", calls)
	}
}

func TestDrainMiddleware_draining(t *testing.T) {
	draining.Store(false)
	StartDraining()
	defer draining.Store(false)

	if !IsDraining() {
		t.Error("IsDraining should report true after StartDraining")
	}

	calls := 0
	p := NewDrainMiddleware(func(_ context.Context, _ *Request) (*Response, error) {
		calls++
		return &Response{IsComplete: true}, nil
	})

	if _, err := p(context.Background(), &Request{}); err != ErrDraining {
		t.Errorf("expecting ErrDraining. Got: %v", err)
	}
	if calls != 0 {
		t.Errorf("the next proxy should not be called while draining. Calls: %d", calls)
	}
}

func TestDrainMiddleware_inFlightRequestsComplete(t *testing.T) {
	draining.Store(false)

	release := make(chan struct{})
	p := NewDrainMiddleware(func(_ context.Context, _ *Request) (*Response, error) {
		<-release
		return &Response{IsComplete: true}, nil
	})

	inFlightDone := make(chan error)
	go func() {
		_, err := p(context.Background(), &Request{})
		inFlightDone <- err
	}()

	// let the in-flight request start and begin draining while it runs
	time.Sleep(50 * time.Millisecond)
	StartDraining()
	defer draining.Store(false)

	// new requests are rejected
	if _, err := p(context.Background(), &Request{}); err != ErrDraining {
		t.Errorf("expecting ErrDraining for new requests while draining. Got: %v", err)
	}

	// the in-flight request completes successfully
	close(release)
	select {
	case err := <-inFlightDone:
		if err != nil {
			t.Errorf("the in-flight request should complete without error. Got: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("the in-flight request did not complete")
	}
}

func TestDefaultFactory_draining(t *testing.T) {
	draining.Store(false)

	backendFactory := func(_ *config.Backend) Proxy {
		return func(_ context.Context, _ *Request) (*Response, error) {
			return &Response{IsComplete: true}, nil
		}
	}
	factory := NewDefaultFactory(backendFactory, logging.NoOp)

	backend := config.Backend{
		URLPattern: "/bar",
		Method:     "GET",
	}
	endpoint := config.EndpointConfig{
		Backend: []*config.Backend{&backend},
	}
	serviceConfig := config.ServiceConfig{
		Version:   config.ConfigVersion,
		Endpoints: []*config.EndpointConfig{&endpoint},
		Timeout:   100 * time.Millisecond,
		Host:      []string{"http://example.com/"},
	}
	if err := serviceConfig.Init(); err != nil {
		t.Fatalf("error during the config init: %s", err.Error())
	}

	p, err := factory.New(&endpoint)
	if err != nil {
		t.Fatalf("unexpected error building the proxy: %v", err)
	}

	URL, err := url.Parse("http://example.com/")
	if err != nil {
		t.Fatalf("building the sample url: %s", err.Error())
	}
	request := &Request{
		Method: "GET",
		Path:   "/foo",
		URL:    URL,
		Body:   newDummyReadCloser(""),
	}

	if _, err = p(context.Background(), request); err != nil {
		t.Errorf("unexpected error before draining: %v", err)
	}

	StartDraining()
	defer draining.Store(false)

	if _, err = p(context.Background(), request); err != ErrDraining {
		t.Errorf("expecting ErrDraining. Got: %v", err)
	}
}
