// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
)

func TestConnectionTracker(t *testing.T) {
	tracker := newConnectionTracker()

	if !tracker.begin() {
		t.Fatal("expected begin to succeed while not draining")
	}
	if !tracker.begin() {
		t.Fatal("expected begin to succeed while not draining")
	}
	if got := tracker.activeRequests(); got != 2 {
		t.Errorf("unexpected number of active requests: %d", got)
	}

	tracker.startDraining()

	if tracker.begin() {
		t.Error("expected begin to fail while draining")
	}
	select {
	case <-tracker.done:
		t.Error("done channel should not be closed while requests are in-flight")
	default:
	}

	tracker.end()
	select {
	case <-tracker.done:
		t.Error("done channel should not be closed while requests are in-flight")
	default:
	}

	tracker.end()
	select {
	case <-tracker.done:
	default:
		t.Error("done channel should be closed once all in-flight requests complete")
	}
	if got := tracker.activeRequests(); got != 0 {
		t.Errorf("unexpected number of active requests: %d", got)
	}
}

func TestConnectionTracker_drainingWithoutInflightRequests(t *testing.T) {
	tracker := newConnectionTracker()
	tracker.startDraining()
	select {
	case <-tracker.done:
	default:
		t.Error("done channel should be closed immediately when no requests are in-flight")
	}
}

func TestConnectionTracker_handler(t *testing.T) {
	tracker := newConnectionTracker()
	handler := tracker.handler(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/foo", nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected status code: %d", recorder.Code)
	}
	if got := tracker.activeRequests(); got != 0 {
		t.Errorf("unexpected number of active requests: %d", got)
	}

	tracker.startDraining()
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/foo", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("unexpected status code while draining: %d", recorder.Code)
	}
}

func TestHealthHandler(t *testing.T) {
	tracker := newConnectionTracker()
	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusTeapot)
	})
	handler := healthHandler("", tracker, next)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, DefaultHealthCheckPath, nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected liveness status code: %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, ReadyCheckPath, nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected readiness status code: %d", recorder.Code)
	}
	assertDrainingFlag(t, recorder.Body.Bytes(), false)

	tracker.startDraining()
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, ReadyCheckPath, nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("unexpected readiness status code while draining: %d", recorder.Code)
	}
	assertDrainingFlag(t, recorder.Body.Bytes(), true)

	// liveness keeps responding while draining
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, DefaultHealthCheckPath, nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected liveness status code while draining: %d", recorder.Code)
	}

	// other paths are delegated to the next handler
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/other", nil))
	if recorder.Code != http.StatusTeapot {
		t.Errorf("unexpected status code for a delegated request: %d", recorder.Code)
	}
}

func TestHealthHandler_customPath(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusTeapot)
	})
	handler := healthHandler("/healthz", newConnectionTracker(), next)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("unexpected liveness status code: %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, DefaultHealthCheckPath, nil))
	if recorder.Code != http.StatusTeapot {
		t.Errorf("the default health path should not be registered: %d", recorder.Code)
	}
}

func assertDrainingFlag(t *testing.T, body []byte, expected bool) {
	t.Helper()
	payload := map[string]interface{}{}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Errorf("unexpected readiness body: %s", err.Error())
		return
	}
	if draining, ok := payload["draining"].(bool); !ok || draining != expected {
		t.Errorf("unexpected draining flag. have %v, want %v", payload["draining"], expected)
	}
}

func TestRunServer_HealthAndReadyEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := newPort()
	done := make(chan error)
	go func() {
		done <- RunServer(ctx, config.ServiceConfig{Port: port}, http.HandlerFunc(dummyHandler))
	}()

	waitForServer(t, port)

	for _, path := range []string{"/health", "/ready"} {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d%s", port, path))
		if err != nil {
			t.Errorf("requesting %s: %s", path, err.Error())
			continue
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status code for %s: %d", path, resp.StatusCode)
		}
		resp.Body.Close()
	}

	cancel()
	if err := <-done; err != nil {
		t.Error(err)
	}
}

// waitForServer polls the liveness endpoint until the server is up
func waitForServer(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the server did not start in time")
}

func TestRunServer_GracefulShutdown_waitsForInflightRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := newPort()
	started := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("complete")) // skipcq: GO-S0907
	})

	done := make(chan error)
	go func() {
		done <- RunServer(ctx, config.ServiceConfig{
			Port:                    port,
			GracefulShutdownTimeout: 5 * time.Second,
		}, handler)
	}()

	waitForServer(t, port)

	type result struct {
		body string
		code int
	}
	responseCh := make(chan result, 1)
	go func() {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/slow", port))
		if err != nil {
			responseCh <- result{body: err.Error(), code: -1}
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		responseCh <- result{body: string(body), code: resp.StatusCode}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight request did not reach the handler")
	}

	// start the graceful shutdown while the request is in-flight
	cancel()

	// give the shutdown a chance to interrupt the in-flight request
	<-time.After(200 * time.Millisecond)
	close(release)

	res := <-responseCh
	if res.code != http.StatusOK {
		t.Errorf("unexpected status code for the in-flight request: %d (%s)", res.code, res.body)
	}
	if res.body != "complete" {
		t.Errorf("the in-flight request was interrupted. body: %q", res.body)
	}

	if err := <-done; err != nil {
		t.Errorf("unexpected shutdown error: %s", err.Error())
	}
}

func TestRunServer_GracefulShutdown_timeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := newPort()
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	handler := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		rw.WriteHeader(http.StatusOK)
	})

	done := make(chan error)
	go func() {
		done <- RunServer(ctx, config.ServiceConfig{
			Port:                    port,
			GracefulShutdownTimeout: 200 * time.Millisecond,
		}, handler)
	}()

	waitForServer(t, port)

	go http.Get(fmt.Sprintf("http://localhost:%d/stuck", port)) // skipcq: GO-S1038
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight request did not reach the handler")
	}

	start := time.Now()
	cancel()

	// the shutdown must not wait for the stuck request longer than the
	// configured graceful shutdown timeout
	<-done
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("the shutdown took too long: %s", elapsed)
	}
}

func TestRunServer_GracefulShutdown_rejectsNewRequestsWhileDraining(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := newPort()
	started := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		rw.WriteHeader(http.StatusOK)
	})

	done := make(chan error)
	go func() {
		done <- RunServer(ctx, config.ServiceConfig{
			Port:                    port,
			GracefulShutdownTimeout: 5 * time.Second,
		}, handler)
	}()

	waitForServer(t, port)

	go http.Get(fmt.Sprintf("http://localhost:%d/slow", port)) // skipcq: GO-S1038
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight request did not reach the handler")
	}
	cancel()

	// the listener is closed while draining, so new connections must fail
	<-time.After(200 * time.Millisecond)
	if _, err := http.Get(fmt.Sprintf("http://localhost:%d/new", port)); err == nil {
		t.Error("expected new connections to be rejected while draining")
	} else if !strings.Contains(err.Error(), "connection refused") {
		t.Logf("new request rejected with: %s", err.Error())
	}

	close(release)
	if err := <-done; err != nil {
		t.Errorf("unexpected shutdown error: %s", err.Error())
	}
}
