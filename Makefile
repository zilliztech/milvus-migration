install-swag:
	go install github.com/swaggo/swag/cmd/swag@v1.8.10

init-swag:
	swag init -d server -g server.go -o ./docs --parseDependency

build-server-for-linux:
	  GO111MODULE=on \
      CGO_ENABLED=0 \
      GOOS=linux \
      GOARCH=amd64 \
      GOPRIVATE="github.com/zilliztech/*" \
      GIT_COMMIT=$$(git rev-parse --short HEAD)  \
      go build -ldflags "-X main._gitHash=$$GIT_COMMIT" -o milvus-migration main.go