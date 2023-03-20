build-server-for-linux:
	  GO111MODULE=on \
      CGO_ENABLED=0 \
      GOOS=linux \
      GOARCH=amd64 \
      GOPRIVATE="github.com/zilliztech/*" \
      GIT_COMMIT=$$(git rev-parse --short HEAD)  \
      go build -ldflags "-X main._gitHash=$$GIT_COMMIT" -o milvus-migration main.go