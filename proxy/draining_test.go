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

func TestNewDrainMiddleware(t *testing.T) {
	defer resetDraining()

	calls := 0
	next := func(_ context.Context, _ *Request) (*Response, error) {
		calls++
		return &Response{IsComplete: true}, nil
	}
	p := NewDrainMiddleware(next)

	if IsDraining() {
		t.Fatal("the proxy layer should not be draining by default")
	}
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
	if calls != 1 {
		t.Errorf("unexpected number of calls to the next proxy: %d", calls)
	}

	StartDraining()
	if !IsDraining() {
		t.Fatal("the proxy layer should be draining")
	}
	if _, err := p(context.Background(), &Request{}); err != ErrServiceDraining {
		t.Errorf("expecting ErrServiceDraining, got: %v", err)
	}
	if calls != 1 {
		t.Errorf("new requests should not reach the next proxy while draining: %d", calls)
	}
}

func TestDefaultFactory_draining(t *testing.T) {
	defer resetDraining()

	backendCalls := 0
	backendFactory := func(_ *config.Backend) Proxy {
		return func(_ context.Context, _ *Request) (*Response, error) {
			backendCalls++
			return &Response{IsComplete: true, Data: map[string]interface{}{"foo": "bar"}}, nil
		}
	}
	factory := NewDefaultFactory(backendFactory, logging.NoOp)

	backend := config.Backend{
		URLPattern: "/foo",
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
		t.Fatalf("initializing the service config: %s", err.Error())
	}

	p, err := factory.New(&endpoint)
	if err != nil {
		t.Fatalf("unexpected factory error: %s", err.Error())
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

	if _, err := p(context.Background(), request); err != nil {
		t.Errorf("unexpected proxy error: %s", err.Error())
	}
	if backendCalls != 1 {
		t.Errorf("unexpected number of backend calls: %d", backendCalls)
	}

	StartDraining()
	if _, err := p(context.Background(), request); err != ErrServiceDraining {
		t.Errorf("expecting ErrServiceDraining, got: %v", err)
	}
	if backendCalls != 1 {
		t.Errorf("new requests should not reach the backend while draining: %d", backendCalls)
	}
}
