// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
)

// pipeListener is an in-memory net.Listener implementation based on net.Pipe,
// so the server tests do not require opening real network sockets.
type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, errors.New("listener closed")
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr("pipe") }

func (l *pipeListener) dial() (net.Conn, error) {
	server, client := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		server.Close()
		client.Close()
		return nil, errors.New("listener closed")
	}
}

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

// runServerInMemory runs runServer attached to an in-memory listener and
// returns a client connected to it along with the channel reporting the
// runServer exit error.
func runServerInMemory(ctx context.Context, cfg config.ServiceConfig, handler http.Handler) (*http.Client, chan error) {
	listener := newPipeListener()
	done := make(chan error, 1)
	go func() {
		done <- runServer(ctx, cfg, handler, nil, func(s *http.Server) error {
			return s.Serve(listener)
		})
	}()
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return listener.dial()
			},
		},
	}
	return client, done
}

func TestConnTracker(t *testing.T) {
	tracker := &connTracker{}
	if tracker.isDraining() {
		t.Error("a new tracker should not be draining")
	}

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := tracker.track(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		<-releaseHandler
		w.WriteHeader(http.StatusOK)
	}))

	go handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	<-handlerEntered

	select {
	case <-tracker.activeRequests():
		t.Error("the tracker should report active requests while the handler is running")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseHandler)
	select {
	case <-tracker.activeRequests():
	case <-time.After(time.Second):
		t.Error("the tracker should report no active requests after the handler completes")
	}

	tracker.startDraining()
	if !tracker.isDraining() {
		t.Error("the tracker should be draining after startDraining")
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("new requests should be rejected while draining. Got status: %d", recorder.Code)
	}
}

func TestHealthCheckHandler(t *testing.T) {
	tracker := &connTracker{}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	handler := healthCheckHandler("", tracker, next)

	// liveness endpoint at the default path
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, DefaultHealthCheckPath, nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected status code for the health endpoint: %d", recorder.Code)
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("unexpected health body: %s", body)
	}

	// readiness endpoint, not draining
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, ReadinessCheckPath, nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected status code for the ready endpoint: %d", recorder.Code)
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"draining":false`) {
		t.Errorf("the ready endpoint should report the gateway is not draining: %s", body)
	}

	// readiness endpoint while draining
	tracker.startDraining()
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, ReadinessCheckPath, nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("the ready endpoint should return 503 while draining. Got: %d", recorder.Code)
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"draining":true`) {
		t.Errorf("the ready endpoint should report the gateway is draining: %s", body)
	}

	// liveness endpoint keeps reporting ok while draining
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, DefaultHealthCheckPath, nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("the health endpoint should keep returning 200 while draining. Got: %d", recorder.Code)
	}

	// any other path is delegated to the next handler
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/other", nil))
	if recorder.Code != http.StatusTeapot {
		t.Errorf("unexpected status code for a delegated request: %d", recorder.Code)
	}
}

func TestHealthCheckHandler_customPath(t *testing.T) {
	tracker := &connTracker{}
	handler := healthCheckHandler("/custom-health", tracker, http.HandlerFunc(dummyHandler))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/custom-health", nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected status code for the custom health endpoint: %d", recorder.Code)
	}
}

func TestRunServer_healthAndReadyEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, done := runServerInMemory(ctx, config.ServiceConfig{}, http.HandlerFunc(dummyHandler))

	for _, tc := range []struct {
		path         string
		expectedCode int
		expectedBody string
	}{
		{"/health", http.StatusOK, `{"status":"ok"}`},
		{"/ready", http.StatusOK, `"draining":false`},
	} {
		resp, err := client.Get("http://pipe" + tc.path)
		if err != nil {
			t.Errorf("GET %s: %s", tc.path, err.Error())
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.expectedCode {
			t.Errorf("GET %s: unexpected status code: %d", tc.path, resp.StatusCode)
		}
		if !strings.Contains(string(body), tc.expectedBody) {
			t.Errorf("GET %s: unexpected body: %s", tc.path, string(body))
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("unexpected error from RunServer: %s", err.Error())
	}
}

func TestRunServer_gracefulShutdownWaitsForInflightRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		<-releaseHandler
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "complete response")
	})

	client, done := runServerInMemory(ctx, config.ServiceConfig{
		GracefulShutdownTimeout: 5 * time.Second,
	}, handler)

	responseReceived := make(chan string, 1)
	go func() {
		resp, err := client.Get("http://pipe/some-endpoint")
		if err != nil {
			responseReceived <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		responseReceived <- string(body)
	}()

	// wait for the request to be in-flight and trigger the shutdown
	<-handlerEntered
	cancel()

	// the server should be draining, waiting for the in-flight request
	select {
	case <-done:
		t.Error("RunServer returned before the in-flight request completed")
	case <-time.After(200 * time.Millisecond):
	}

	// let the in-flight request finish: the client must get the complete response
	close(releaseHandler)

	select {
	case body := <-responseReceived:
		if body != "complete response" {
			t.Errorf("the client received an incomplete response: %q", body)
		}
	case <-time.After(2 * time.Second):
		t.Error("the in-flight request did not complete")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error from RunServer: %s", err.Error())
		}
	case <-time.After(2 * time.Second):
		t.Error("RunServer did not return after the in-flight request completed")
	}
}

func TestRunServer_gracefulShutdownTimeoutForcesClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerEntered := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		// block until the request is forcefully cancelled
		<-r.Context().Done()
	})

	client, done := runServerInMemory(ctx, config.ServiceConfig{
		GracefulShutdownTimeout: 200 * time.Millisecond,
	}, handler)

	clientDone := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://pipe/some-endpoint")
		if err != nil {
			clientDone <- err
			return
		}
		resp.Body.Close()
		clientDone <- nil
	}()

	<-handlerEntered
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expecting a deadline exceeded error after the drain timeout. Got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("RunServer did not force the shutdown after the drain timeout")
	}

	// the in-flight request was interrupted: the client did not get a complete response
	select {
	case err := <-clientDone:
		if err == nil {
			t.Error("the client should not receive a complete response after a forced shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Error("the client request was not interrupted after the forced shutdown")
	}
}
