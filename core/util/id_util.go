package util

import (
	"github.com/matoous/go-nanoid/v2"
)

var (
	defaultSize = 21
)

var defaultAlphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func GenerateUUID(prefix string) string {
	return prefix + gonanoid.MustGenerate(defaultAlphabet, defaultSize)
}
