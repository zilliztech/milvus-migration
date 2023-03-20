package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestService(t *testing.T) {

	server, err := NewServer()
	assert.NoError(t, err)

	server.Init()
	server.Start()
	time.Sleep(1000 * time.Second)
}
