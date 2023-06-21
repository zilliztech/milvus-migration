package storage

import (
	"fmt"

	aliyunCred "github.com/aliyun/credentials-go/credentials"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
)

const _aliyunEndpoint = "oss.aliyuncs.com"

type credentialProvider struct {
	// aliyunCred doesn't provide a way to get the expire time, so we use the cache to check if it's expired
	// when aliyunCreds.GetAccessKeyId is different from the cache, we know it's expired
	akCache    string
	aliyunCred aliyunCred.Credential
}

func newCredentialProvider() (*credentialProvider, error) {
	cred, err := aliyunCred.NewCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("create aliyun credential %w", err)
	}

	return &credentialProvider{aliyunCred: cred}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (c *credentialProvider) Retrieve() (minioCred.Value, error) {
	var ret minioCred.Value
	ak, err := c.aliyunCred.GetAccessKeyId()
	if err != nil {
		return ret, fmt.Errorf("get access key id from aliyun credential %w", err)
	}
	ret.AccessKeyID = *ak
	sk, err := c.aliyunCred.GetAccessKeySecret()
	if err != nil {
		return minioCred.Value{}, fmt.Errorf("get access key secret from aliyun credential %w", err)
	}
	securityToken, err := c.aliyunCred.GetSecurityToken()
	if err != nil {
		return minioCred.Value{}, fmt.Errorf("get security token from aliyun credential %w", err)
	}
	ret.SecretAccessKey = *sk
	c.akCache = *ak
	ret.SessionToken = *securityToken
	return ret, nil
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (c *credentialProvider) IsExpired() bool {
	ak, err := c.aliyunCred.GetAccessKeyId()
	if err != nil {
		return true
	}
	return *ak != c.akCache
}

func NewAliyunClient(cfg Cfg) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region, BucketLookup: minio.BucketLookupDNS}
	if cfg.UseIAM {
		provider, err := newCredentialProvider()
		if err != nil {
			return nil, fmt.Errorf("storage: new aliyun credential provider %w", err)
		}
		opts.Creds = minioCred.New(provider)
	} else {
		opts.Creds = minioCred.NewStaticV4(cfg.AK, cfg.SK, "")
	}

	addr := fmt.Sprintf("%s.%s", cfg.Region, _aliyunEndpoint)
	cli, err := minio.New(addr, &opts)
	if err != nil {
		return nil, fmt.Errorf("storage: new aliyun client %w", err)
	}

	return &MinioClient{cli: cli, provider: ALI}, nil
}
