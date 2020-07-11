package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	"github.com/milanaleksic/tt-extractor-kindle/oreilly"
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
	if err != nil {
		log.Fatalf("failed to open cookie JSON file, please look at the documentation how to make one: %v", err)
	}
	err = json.Unmarshal(cookiesBytes, &cookies)
	if err != nil {
		log.Fatalf("failed to read the cookies JAR file, please look at the documentation how to make one: %v", err)
	}

	contentExtractor, err := oreilly.NewContentExtractor(
		model.NewBookRepository(db),
		model.NewAnnotationRepository(db),
		cookies,
	)
	if err != nil {
		log.Fatalf("failed to create content extractor: %v", err)
	}
	err = contentExtractor.IngestRecords()
	if err != nil {
		log.Fatalf("failed ingesting annotations in oreilly learning platform: %v", err)
	}
}

func prepareDatabase() *sql.DB {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	return db
}
