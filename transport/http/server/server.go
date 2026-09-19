// SPDX-License-Identifier: Apache-2.0

/*
Package server provides tools to create http servers and handlers wrapping the lura router
*/
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/core"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// ToHTTPError translates an error into a HTTP status code
type ToHTTPError func(error) int

// DefaultToHTTPError is a ToHTTPError transalator that always returns an
// internal server error
func DefaultToHTTPError(_ error) int {
	return http.StatusInternalServerError
}

const (
	// HeaderCompleteResponseValue is the value of the CompleteResponseHeader
	// if the response is complete
	HeaderCompleteResponseValue = "true"
	// HeaderIncompleteResponseValue is the value of the CompleteResponseHeader
	// if the response is not complete
	HeaderIncompleteResponseValue = "false"
)

var (
	// CompleteResponseHeaderName is the header to flag incomplete responses to the client
	CompleteResponseHeaderName = "X-Krakend-Completed"
	// HeadersToSend are the headers to pass from the router request to the proxy
	HeadersToSend = []string{"Content-Type"}
	// UserAgentHeaderValue is the value of the User-Agent header to add to the proxy request
	UserAgentHeaderValue = []string{core.KrakendUserAgent}

	// ErrInternalError is the error returned by the router when something went wrong
	ErrInternalError = errors.New("internal server error")
	// ErrPrivateKey is the error returned by the router when the private key is not defined
	ErrPrivateKey = errors.New("private key not defined")
	// ErrPublicKey is the error returned by the router when the public key is not defined
	ErrPublicKey = errors.New("public key not defined")
	loggerPrefix = "[SERVICE: HTTP Server]"
)

const (
	// DefaultHealthCheckPath is the default path for the liveness endpoint
	DefaultHealthCheckPath = "/health"
	// ReadyCheckPath is the path for the readiness endpoint. It reports
	// whether the gateway is draining
	ReadyCheckPath = "/ready"
)

// connectionTracker tracks the in-flight requests being processed by the
// server so the graceful shutdown can wait for them to complete before
// forcefully closing the remaining connections
type connectionTracker struct {
	mu       sync.Mutex
	active   int64
	draining atomic.Bool
	done     chan struct{}
	closed   bool
}

func newConnectionTracker() *connectionTracker {
	return &connectionTracker{done: make(chan struct{})}
}

// begin registers a new in-flight request. It returns false if the server
// is draining and no new requests should be accepted
func (t *connectionTracker) begin() bool {
	if t.draining.Load() {
		return false
	}
	t.mu.Lock()
	t.active++
	t.mu.Unlock()
	return true
}

// end unregisters an in-flight request. When the server is draining and the
// last in-flight request finishes, the done channel is closed
func (t *connectionTracker) end() {
	t.mu.Lock()
	t.active--
	if t.draining.Load() && t.active == 0 && !t.closed {
		t.closed = true
		close(t.done)
	}
	t.mu.Unlock()
}

// startDraining flags the tracker as draining so no new requests are
// accepted. If there are no in-flight requests, the done channel is closed
func (t *connectionTracker) startDraining() {
	t.draining.Store(true)
	t.mu.Lock()
	if t.active == 0 && !t.closed {
		t.closed = true
		close(t.done)
	}
	t.mu.Unlock()
}

// activeRequests returns the number of in-flight requests
func (t *connectionTracker) activeRequests() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}

// isDraining reports whether the tracker is draining
func (t *connectionTracker) isDraining() bool {
	return t.draining.Load()
}

// handler wraps the received handler so every request is tracked. Requests
// received while draining are rejected with a 503 status code
func (t *connectionTracker) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if !t.begin() {
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte(`{"error":"service is shutting down"}`)) // skipcq: GO-S0907
			return
		}
		defer t.end()
		next.ServeHTTP(rw, req)
	})
}

// healthHandler exposes the liveness and readiness endpoints and delegates
// any other request to the received handler
func healthHandler(healthPath string, tracker *connectionTracker, next http.Handler) http.Handler {
	if healthPath == "" {
		healthPath = DefaultHealthCheckPath
	}
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case healthPath:
			writeHealthResponse(rw, http.StatusOK, map[string]interface{}{
				"status": "ok",
			})
		case ReadyCheckPath:
			draining := tracker.isDraining()
			status := http.StatusOK
			state := "ready"
			if draining {
				status = http.StatusServiceUnavailable
				state = "draining"
			}
			writeHealthResponse(rw, status, map[string]interface{}{
				"status":   state,
				"draining": draining,
			})
		default:
			next.ServeHTTP(rw, req)
		}
	})
}

func writeHealthResponse(rw http.ResponseWriter, status int, body map[string]interface{}) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	json.NewEncoder(rw).Encode(body) // skipcq: GO-S0907
}

// InitHTTPDefaultTransport ensures the default HTTP transport is configured just once per execution
func InitHTTPDefaultTransport(cfg config.ServiceConfig) {
	InitHTTPDefaultTransportWithLogger(cfg, nil)
}

func InitHTTPDefaultTransportWithLogger(cfg config.ServiceConfig, logger logging.Logger) {
	if logger == nil {
		logger = logging.NoOp
	}
	if cfg.AllowInsecureConnections {
		if cfg.ClientTLS == nil {
			cfg.ClientTLS = &config.ClientTLS{}
		}
		cfg.ClientTLS.AllowInsecureConnections = true
	}
	onceTransportConfig.Do(func() {
		http.DefaultTransport = NewTransport(cfg, logger)
	})
}

func NewTransport(cfg config.ServiceConfig, logger logging.Logger) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:       cfg.DialerTimeout,
			KeepAlive:     cfg.DialerKeepAlive,
			FallbackDelay: cfg.DialerFallbackDelay,
			DualStack:     true,
		}).DialContext,
		DisableCompression:    cfg.DisableCompression,
		DisableKeepAlives:     cfg.DisableKeepAlives,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: cfg.ExpectContinueTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		TLSClientConfig:       ParseClientTLSConfigWithLogger(cfg.ClientTLS, logger),
	}
}

// RunServer runs a http.Server with the given handler and configuration.
// It configures the TLS layer if required by the received configuration.
func RunServer(ctx context.Context, cfg config.ServiceConfig, handler http.Handler) error {
	return RunServerWithLoggerFactory(nil)(ctx, cfg, handler)
}

func RunServerWithLoggerFactory(l logging.Logger) func(context.Context, config.ServiceConfig, http.Handler) error {
	return func(ctx context.Context, cfg config.ServiceConfig, handler http.Handler) error {
		done := make(chan error)
		tracker := newConnectionTracker()
		s := NewServerWithLogger(cfg, healthHandler(cfg.HealthCheckPath, tracker, tracker.handler(handler)), l)

		if s.TLSConfig == nil {
			go func() {
				done <- s.ListenAndServe()
			}()
		} else {
			if cfg.TLS.PublicKey != "" || cfg.TLS.PrivateKey != "" {
				cfg.TLS.Keys = append(cfg.TLS.Keys, config.TLSKeyPair{
					PublicKey:  cfg.TLS.PublicKey,
					PrivateKey: cfg.TLS.PrivateKey,
				})
			}
			if len(cfg.TLS.Keys) == 0 {
				return ErrPublicKey
			}
			for _, k := range cfg.TLS.Keys {
				if k.PublicKey == "" {
					return ErrPublicKey
				}
				if k.PrivateKey == "" {
					return ErrPrivateKey
				}
				cert, err := tls.LoadX509KeyPair(k.PublicKey, k.PrivateKey)
				if err != nil {
					return err
				}
				s.TLSConfig.Certificates = append(s.TLSConfig.Certificates, cert)
			}

			go func() {
				// since we already use the list of certificates in the config
				// we do not need to specify the files for public and private key here
				done <- s.ListenAndServeTLS("", "")
			}()
		}

		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return gracefulShutdown(s, tracker, cfg, l)
		}
	}
}

// gracefulShutdown stops accepting new connections, waits for all the
// in-flight requests to complete within the configured timeout and, once
// the timeout expires, forcefully closes the remaining connections
func gracefulShutdown(s *http.Server, tracker *connectionTracker, cfg config.ServiceConfig, l logging.Logger) error {
	if l == nil {
		l = logging.NoOp
	}
	// flag the proxy layer and the connection tracker as draining so no new
	// requests are accepted while the in-flight ones complete
	proxy.StartDraining()
	tracker.startDraining()
	l.Info(fmt.Sprintf("%s Draining %d in-flight request(s)", loggerPrefix, tracker.activeRequests()))

	timeout := cfg.GracefulShutdownTimeout
	if timeout <= 0 {
		timeout = cfg.MaxShutdownDuration
	}

	shutdownCtx := context.Background()
	cancel := func() {}
	if timeout > 0 {
		shutdownCtx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	defer cancel()

	// stop accepting new connections and wait for the in-flight requests
	shutdownErr := s.Shutdown(shutdownCtx)

	// wait for all the tracked in-flight requests to complete within the
	// configured timeout. On timeout, forcefully close the remaining
	// connections
	select {
	case <-tracker.done:
		l.Info(loggerPrefix + " All in-flight requests completed")
	case <-shutdownCtx.Done():
		l.Warning(fmt.Sprintf("%s Graceful shutdown timeout (%s) exceeded, closing connections", loggerPrefix, timeout))
		if err := s.Close(); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	return shutdownErr
}

// NewServer returns a http.Server ready to serve the injected handler
func NewServer(cfg config.ServiceConfig, handler http.Handler) *http.Server {
	return NewServerWithLogger(cfg, handler, nil)
}

func NewServerWithLogger(cfg config.ServiceConfig, handler http.Handler, logger logging.Logger) *http.Server {
	if cfg.UseH2C {
		handler = h2c.NewHandler(handler, &http2.Server{})
	}

	return &http.Server{
		Addr:              net.JoinHostPort(cfg.Address, fmt.Sprintf("%d", cfg.Port)),
		Handler:           handler,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		TLSConfig:         ParseTLSConfigWithLogger(cfg.TLS, logger),
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
}

// ParseTLSConfig creates a tls.Config from the TLS section of the service configuration
func ParseTLSConfig(cfg *config.TLS) *tls.Config {
	return ParseTLSConfigWithLogger(cfg, nil)
}

func ParseTLSConfigWithLogger(cfg *config.TLS, logger logging.Logger) *tls.Config {
	if cfg == nil {
		return nil
	}
	if cfg.IsDisabled {
		return nil
	}

	if logger == nil {
		logger = logging.NoOp
	}

	tlsConfig := &tls.Config{
		MinVersion:       parseTLSVersion(cfg.MinVersion),
		MaxVersion:       parseTLSVersion(cfg.MaxVersion),
		CurvePreferences: parseCurveIDs(cfg.CurvePreferences),
		CipherSuites:     parseCipherSuites(cfg.CipherSuites),
	}
	if !cfg.EnableMTLS {
		return tlsConfig
	}

	certPool := loadCertPool(cfg.DisableSystemCaPool, cfg.CaCerts, logger)

	for _, cert := range cfg.Keys {
		caCert, err := os.ReadFile(cert.PublicKey)
		if err != nil {
			logger.Error(fmt.Sprintf("%s Cannot load public key %s: %s", loggerPrefix, cert.PublicKey, err.Error()))
			continue
		}
		certPool.AppendCertsFromPEM(caCert)
	}

	tlsConfig.ClientCAs = certPool
	tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert

	return tlsConfig
}

func ParseClientTLSConfigWithLogger(cfg *config.ClientTLS, logger logging.Logger) *tls.Config {
	if cfg == nil {
		return nil
	}
	return &tls.Config{
		InsecureSkipVerify: cfg.AllowInsecureConnections,
		RootCAs:            loadCertPool(cfg.DisableSystemCaPool, cfg.CaCerts, logger),
		MinVersion:         parseTLSVersion(cfg.MinVersion),
		MaxVersion:         parseTLSVersion(cfg.MaxVersion),
		CurvePreferences:   parseCurveIDs(cfg.CurvePreferences),
		CipherSuites:       parseCipherSuites(cfg.CipherSuites),
		Certificates:       loadClientCerts(cfg.ClientCerts, logger),
	}
}

func loadCertPool(disableSystemCaPool bool, caCerts []string, logger logging.Logger) *x509.CertPool {
	certPool := x509.NewCertPool()
	if !disableSystemCaPool {
		if systemCertPool, err := x509.SystemCertPool(); err == nil {
			certPool = systemCertPool
		} else {
			logger.Error(fmt.Sprintf("%s Cannot load system CA pool: %s", loggerPrefix, err.Error()))
		}
	}

	for _, path := range caCerts {
		if ca, err := os.ReadFile(path); err == nil {
			certPool.AppendCertsFromPEM(ca)
		} else {
			logger.Error(fmt.Sprintf("%s Cannot load certificate CA %s: %s", loggerPrefix, path, err.Error()))
		}
	}
	return certPool
}

func loadClientCerts(certFiles []config.ClientTLSCert, logger logging.Logger) []tls.Certificate {
	certs := make([]tls.Certificate, 0, len(certFiles))
	for _, certAndKey := range certFiles {
		cert, err := tls.LoadX509KeyPair(certAndKey.Certificate, certAndKey.PrivateKey)
		if err != nil {
			logger.Error(fmt.Sprintf("%s Cannot load client certificate %s, %s: %s",
				loggerPrefix, certAndKey.Certificate, certAndKey.PrivateKey, err.Error()))
			continue
		}
		certs = append(certs, cert)
	}

	return certs
}

func parseTLSVersion(key string) uint16 {
	if v, ok := versions[key]; ok {
		return v
	}
	return tls.VersionTLS13
}

func parseCurveIDs(curvePreferences []uint16) []tls.CurveID {
	l := len(curvePreferences)
	if l == 0 {
		return defaultCurves
	}

	curves := make([]tls.CurveID, len(curvePreferences))
	for i := range curves {
		curves[i] = tls.CurveID(curvePreferences[i])
	}
	return curves
}

func parseCipherSuites(cipherSuites []uint16) []uint16 {
	l := len(cipherSuites)
	if l == 0 {
		return defaultCipherSuites
	}

	cs := make([]uint16, l)
	for i := range cs {
		cs[i] = uint16(cipherSuites[i])
	}
	return cs
}

var (
	onceTransportConfig sync.Once
	defaultCurves       = []tls.CurveID{
		tls.CurveP521,
		tls.CurveP384,
		tls.CurveP256,
	}
	defaultCipherSuites = []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	}
	versions = map[string]uint16{
		"SSL3.0": tls.VersionSSL30,
		"TLS10":  tls.VersionTLS10,
		"TLS11":  tls.VersionTLS11,
		"TLS12":  tls.VersionTLS12,
		"TLS13":  tls.VersionTLS13,
	}
)
