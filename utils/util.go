package utils

import (
	log "github.com/sirupsen/logrus"
	"strconv"
)

func MustItoa(s string) *int {
	if s == "" {
		return nil
	}
	result, err := strconv.Atoi(s)
	Check(err)
	return &result
}

func Check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
