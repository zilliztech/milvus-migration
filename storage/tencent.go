package storage

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

// NewTencentClient returns a minio.Client which is compatible for tencent COS
func NewTencentClient(cfg Cfg) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region, BucketLookup: minio.BucketLookupDNS}
	if cfg.UseIAM {
		provider, err := newTcCredentialProvider()
		if err != nil {
			return nil, fmt.Errorf("storage: new tencent credential provider %w", err)
		}
		opts.Creds = minioCred.New(provider)
	} else {
		opts.Creds = minioCred.NewStaticV4(cfg.AK, cfg.SK, "")
	}

	var addr string
	if len(cfg.Endpoint) <= 0 {
		addr = fmt.Sprintf("cos.%s.myqcloud.com", opts.Region)
		opts.Secure = true
	} else {
		addr = cfg.Endpoint
	}
	cli, err := minio.New(addr, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: new tencent client %w", err)
	}

	return &MinioClient{cli: cli, provider: TC}, nil
}

// Credential is defined to mock tencent credential.Credentials
//
//go:generate mockery --name=Credential --with-expecter
type Credential interface {
	common.CredentialIface
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type CredentialProvider struct {
	// tencentCreds doesn't provide a way to get the expired time, so we use the cache to check if it's expired
	// when tencentCreds.GetSecretId is different from the cache, we know it's expired
	akCache      string
	tencentCreds Credential
}

func newTcCredentialProvider() (minioCred.Provider, error) {
	provider, err := common.DefaultTkeOIDCRoleArnProvider()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create tencent credential provider")
	}

	cred, err := provider.GetCredential()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get tencent credential")
	}
	return &CredentialProvider{tencentCreds: cred}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (c *CredentialProvider) Retrieve() (minioCred.Value, error) {
	ret := minioCred.Value{}
	ak := c.tencentCreds.GetSecretId()
	ret.AccessKeyID = ak
	c.akCache = ak

	sk := c.tencentCreds.GetSecretKey()
	ret.SecretAccessKey = sk

	securityToken := c.tencentCreds.GetToken()
	ret.SessionToken = securityToken
	return ret, nil
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (c CredentialProvider) IsExpired() bool {
	ak := c.tencentCreds.GetSecretId()
	return ak != c.akCache
}
