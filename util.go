package tt_extractor_kindle

import (
	log "github.com/sirupsen/logrus"
	"strconv"
)

func MustItoa(s string) *int {
	if s == "" {
		return nil
	}
	result, err := strconv.Atoi(s)
	check(err)
	return &result
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
