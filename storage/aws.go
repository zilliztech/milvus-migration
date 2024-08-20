package storage

import (
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const _awsEndpoint = "s3.amazonaws.com"
const _awsChinaEndpoint = "s3.amazonaws.com.cn"

func NewAWSClient(cfg Cfg) (*MinioClient, error) {
	opts := minio.Options{Secure: cfg.UseSSL, Region: cfg.Region}

	opts.Creds = func() *credentials.Credentials {
		if cfg.UseIAM {
			return credentials.NewIAM("")
		}

		// for aws, if ak/sk is empty, we use anonymous credential
		if len(cfg.AK) == 0 || len(cfg.SK) == 0 {
			return &credentials.Credentials{}
		}

		return credentials.NewStaticV4(cfg.AK, cfg.SK, "")
	}()

	var cli *minio.Client
	var err error
	if len(cfg.Endpoint) != 0 {
		cli, err = minio.New(cfg.Endpoint, &opts)
	} else {
		isCNRegion := strings.HasPrefix(cfg.Region, "cn-")
		if isCNRegion {
			cli, err = minio.New(_awsChinaEndpoint, &opts)
		} else {
			cli, err = minio.New(_awsEndpoint, &opts)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("storage: new aws client %w", err)
	}

	return &MinioClient{cli: cli, provider: AWS}, nil
}
