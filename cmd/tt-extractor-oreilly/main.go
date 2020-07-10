package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	"github.com/milanaleksic/tt-extractor-kindle/oreilly"
	"github.com/milanaleksic/tt-extractor-kindle/utils"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
)

var (
	cookiesFile      string
	databaseLocation string
)

func init() {
	var debug bool
	flag.StringVar(&cookiesFile, "cookies", "cookies.json", "Cookies copied from the browser")
	flag.StringVar(&databaseLocation, "database", "clippings.db", "SQLite3 database location")
	flag.BoolVar(&debug, "debug", false, "show debug messages")
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
}

func main() {
	db := prepareDatabase()
	defer db.Close()

	var cookies map[string]string
	cookiesBytes, err := ioutil.ReadFile(cookiesFile)
	utils.Check(err)
	err = json.Unmarshal(cookiesBytes, &cookies)
	utils.Check(err)

	contentExtractor := oreilly.NewContentExtractor(
		model.NewBookRepository(db),
		model.NewAnnotationRepository(db),
		cookies,
	)
	contentExtractor.IngestRecords()
}

func prepareDatabase() *sql.DB {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	return db
}
