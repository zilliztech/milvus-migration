package storage

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const _gcpEndpoint = "storage.googleapis.com"
const (
	_xAmzPrefix  = "X-Amz-"
	_xGoogPrefix = "X-Goog-"
)

// WrapHTTPTransport wraps http.Transport, add an auth header to support GCP native auth
type wrapHTTPTransport struct {
	tokenSrc     oauth2.TokenSource
	backend      transport
	currentToken atomic.Pointer[oauth2.Token]
}

// transport abstracts http.Transport to simplify test
type transport interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

// newWrapHTTPTransport constructs a new WrapHTTPTransport
func newWrapHTTPTransport(secure bool) (*wrapHTTPTransport, error) {
	tokenSrc := google.ComputeTokenSource("")
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("storage: create default transport %w", err)
	}
	return &wrapHTTPTransport{tokenSrc: tokenSrc, backend: backend}, nil
}

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
func (t *wrapHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, v := range req.Header {
		if strings.HasPrefix(k, _xAmzPrefix) {
			req.Header[strings.Replace(k, _xAmzPrefix, _xGoogPrefix, 1)] = v
			delete(req.Header, k)
		}
	}
	// here Valid() means the token won't be expired in 10 sec
	// so the http client timeout shouldn't be longer, or we need to change the default `expiryDelta` time
	currentToken := t.currentToken.Load()
	if currentToken.Valid() {
		req.Header.Set("Authorization", "Bearer "+currentToken.AccessToken)
	} else {
		newToken, err := t.tokenSrc.Token()
		if err != nil {
			return nil, fmt.Errorf("storage: acquire token %w", err)
		}
		t.currentToken.Store(newToken)
		req.Header.Set("Authorization", "Bearer "+newToken.AccessToken)
	}

	return t.backend.RoundTrip(req)
}

func NewGCPClient(cfg Cfg) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region}
	if cfg.UseIAM {
		trans, err := newWrapHTTPTransport(cfg.UseSSL)
		if err != nil {
			return nil, err
		}
		opts.Transport = trans
		opts.Creds = credentials.NewStaticV2("", "", "")
	} else {
		if len(opts.Region) == 0 {
			// region can not be empty
			opts.Region = "some-region"
		}
		opts.Creds = credentials.NewStaticV2(cfg.AK, cfg.SK, "")
	}

	cli, err := minio.New(_gcpEndpoint, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: new gcp client %w", err)
	}

	return &MinioClient{cli: cli, provider: GCP}, nil
}
