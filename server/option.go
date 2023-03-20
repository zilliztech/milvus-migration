package server

import "strings"

type ServerConfig struct {
	port string
}

func newDefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		port: ":8080",
	}
}

// BackupOption is used to config the retry function.
type ServerOption func(*ServerConfig)

func Port(port string) ServerOption {
	return func(c *ServerConfig) {
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}
		c.port = port
	}
}
