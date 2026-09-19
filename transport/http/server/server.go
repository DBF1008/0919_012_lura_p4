// SPDX-License-Identifier: Apache-2.0

/*
Package server provides tools to create http servers and handlers wrapping the lura router
*/
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	// DefaultHealthCheckPath is the path where the liveness endpoint is
	// exposed when the configuration does not define one
	DefaultHealthCheckPath = "/health"
	// ReadinessCheckPath is the path where the readiness endpoint is exposed.
	// It reports whether the gateway is draining
	ReadinessCheckPath = "/ready"
)

// connTracker tracks the in-flight requests being served and whether the
// server is draining, so the graceful shutdown can wait for the active
// requests to complete and the readiness endpoint can report the drain state.
type connTracker struct {
	active   sync.WaitGroup
	draining atomic.Bool
}

// isDraining reports whether the server is draining (shutting down)
func (t *connTracker) isDraining() bool {
	return t.draining.Load()
}

// startDraining flags the server as draining, so new requests are rejected
func (t *connTracker) startDraining() {
	t.draining.Store(true)
}

// activeRequests returns the number of in-flight requests being served
func (t *connTracker) activeRequests() chan struct{} {
	done := make(chan struct{})
	go func() {
		t.active.Wait()
		close(done)
	}()
	return done
}

// track wraps a handler so every served request is accounted for until it
// completes. Requests arriving while draining are rejected with a 503 error.
func (t *connTracker) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if t.isDraining() {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		t.active.Add(1)
		defer t.active.Done()
		next.ServeHTTP(w, r)
	})
}

// healthCheckHandler serves the liveness (health check) and readiness
// endpoints, delegating any other request to the wrapped handler. The
// readiness endpoint reports whether the gateway is draining.
func healthCheckHandler(healthCheckPath string, t *connTracker, next http.Handler) http.Handler {
	if healthCheckPath == "" {
		healthCheckPath = DefaultHealthCheckPath
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case healthCheckPath:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"ok"}`)
		case ReadinessCheckPath:
			w.Header().Set("Content-Type", "application/json")
			if t.isDraining() {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"status":"draining","draining":true}`)
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"ready","draining":false}`)
		default:
			next.ServeHTTP(w, r)
		}
	})
}

// gracefulShutdown drains the server: it flags the gateway as draining so new
// requests are rejected, stops accepting new connections and waits for all the
// in-flight requests to complete within the configured timeout. If the timeout
// is exceeded, the remaining connections are forcefully closed.
func gracefulShutdown(cfg config.ServiceConfig, s *http.Server, t *connTracker) error {
	t.startDraining()
	proxy.StartDraining()

	timeout := cfg.GracefulShutdownTimeout
	if timeout <= 0 {
		timeout = cfg.MaxShutdownDuration
	}

	ctx := context.Background()
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	// Shutdown stops accepting new connections and blocks until all the
	// in-flight requests complete or the context deadline is exceeded.
	err := s.Shutdown(ctx)
	if err != nil {
		// the drain timeout was exceeded: force-close the remaining connections
		_ = s.Close()
		return err
	}
	// ensure all the tracked in-flight requests have completed
	<-t.activeRequests()
	return nil
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
		return runServer(ctx, cfg, handler, l, nil)
	}
}

// runServer runs the given server handler until the context is cancelled or
// the server stops, shutting it down gracefully. If serve is nil, the serve
// function is built from the configuration (with or without TLS).
func runServer(ctx context.Context, cfg config.ServiceConfig, handler http.Handler, l logging.Logger, serve func(*http.Server) error) error {
	done := make(chan error)
	tracker := &connTracker{}
	handler = healthCheckHandler(cfg.HealthCheckPath, tracker, tracker.track(handler))
	s := NewServerWithLogger(cfg, handler, l)

	if serve == nil {
		var err error
		serve, err = serveFunc(s, cfg)
		if err != nil {
			return err
		}
	}

	go func() {
		done <- serve(s)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return gracefulShutdown(cfg, s, tracker)
	}
}

// serveFunc returns the function that runs the server, loading the TLS
// certificates into the server TLS configuration if required.
func serveFunc(s *http.Server, cfg config.ServiceConfig) (func(*http.Server) error, error) {
	if s.TLSConfig == nil {
		return (*http.Server).ListenAndServe, nil
	}
	if cfg.TLS.PublicKey != "" || cfg.TLS.PrivateKey != "" {
		cfg.TLS.Keys = append(cfg.TLS.Keys, config.TLSKeyPair{
			PublicKey:  cfg.TLS.PublicKey,
			PrivateKey: cfg.TLS.PrivateKey,
		})
	}
	if len(cfg.TLS.Keys) == 0 {
		return nil, ErrPublicKey
	}
	for _, k := range cfg.TLS.Keys {
		if k.PublicKey == "" {
			return nil, ErrPublicKey
		}
		if k.PrivateKey == "" {
			return nil, ErrPrivateKey
		}
		cert, err := tls.LoadX509KeyPair(k.PublicKey, k.PrivateKey)
		if err != nil {
			return nil, err
		}
		s.TLSConfig.Certificates = append(s.TLSConfig.Certificates, cert)
	}

	// since we already use the list of certificates in the config
	// we do not need to specify the files for public and private key here
	return func(s *http.Server) error { return s.ListenAndServeTLS("", "") }, nil
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
