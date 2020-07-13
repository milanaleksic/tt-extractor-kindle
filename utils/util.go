package utils

import (
	log "github.com/sirupsen/logrus"
	"io"
	"strconv"
)

func MustItoa(s string) *int {
	if s == "" {
		return nil
	}
	result, err := strconv.Atoi(s)
	MustCheck(err)
	return &result
}

func MustCheck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func SafeClose(c io.Closer, err *error) {
	if cerr := c.Close(); cerr != nil && *err == nil {
		*err = cerr
	}
}
