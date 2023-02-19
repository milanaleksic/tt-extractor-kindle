package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	"github.com/milanaleksic/tt-extractor-kindle/oreilly"
	log "github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
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
	defer func() {
		if err := db.Close(); err != nil {
			log.Errorf("Failed to close the database connection: %w", err)
		}
	}()

	contentExtractor := oreilly.NewContentExtractor(
		model.NewDBBookRepository(db),
		model.NewDBAnnotationRepository(db),
	)

	ctx := context.Background()

	f, err := os.Open(csvInput)
	if err != nil {
		log.Fatalf("Failed to open input file: %s, reason: %v", csvInput, err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Warnf("Failed to close file %v, err=%v", f, err)
		}
	}()
	err = contentExtractor.IngestRecords(ctx, f)
	if err != nil {
		log.Fatalf("failed ingesting annotations in oreilly learning platform: %v", err)
	}
}

func prepareDatabase() *sql.DB {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?cache=shared&journal_mode=WAL", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	return db
}
