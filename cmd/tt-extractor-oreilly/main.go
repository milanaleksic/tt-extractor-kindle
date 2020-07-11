package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	"github.com/milanaleksic/tt-extractor-kindle/oreilly"
	log "github.com/sirupsen/logrus"
	"os"
)

var (
	csvInput         string
	databaseLocation string
)

func init() {
	var debug bool
	flag.StringVar(&csvInput, "csv", "safari-annotations-export.csv", "Exported annotations CSV file")
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

	contentExtractor := oreilly.NewContentExtractor(
		model.NewBookRepository(db),
		model.NewAnnotationRepository(db),
	)

	f, err := os.Open(csvInput)
	if err != nil {
		log.Fatalf("Failed to open input file: %s, reason: %v", csvInput, err)
	}
	defer f.Close()
	err = contentExtractor.IngestRecords(f)
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
