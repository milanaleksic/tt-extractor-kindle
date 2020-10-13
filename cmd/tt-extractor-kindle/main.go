package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/milanaleksic/tt-extractor-kindle/kindle"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

type inputFiles []string

func (i *inputFiles) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *inputFiles) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var (
	inputFileLocations inputFiles
	databaseLocation   string
)

func init() {
	var debug bool
	flag.Var(&inputFileLocations, "input-file", "input clipping files")
	flag.StringVar(&databaseLocation, "database", "clippings.db", "SQLite3 database location")
	flag.BoolVar(&debug, "debug", false, "show debug messages")
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	if inputFileLocations != nil {
		for _, inputFileLocation := range inputFileLocations {
			if _, err := os.Stat(inputFileLocation); os.IsNotExist(err) {
				log.Fatalf("Input file does not exist: %s", inputFileLocation)
			}
		}
	}
}

func main() {
	db := prepareDatabase()
	defer func() {
		if err := db.Close(); err != nil {
			log.Errorf("Failed to close the database database: %w", err)
		}
	}()

	var openedFiles []io.Closer
	defer func() {
		for _, f := range openedFiles {
			err := f.Close()
			if err != nil {
				log.Warnf("Failed to close file %v, err=%v", f, err)
			}
		}
	}()

	ctx := context.Background()

	if len(inputFileLocations) > 0 {
		for _, inputFileLocation := range inputFileLocations {
			f, err := os.Open(inputFileLocation)
			if err != nil {
				log.Fatalf("Failed to open input file: %s, reason: %v", inputFileLocation, err)
			}
			openedFiles = append(openedFiles, f)

			contentExtractor := kindle.NewContentExtractor(
				model.NewDBBookRepository(db),
				model.NewDBAnnotationRepository(db),
				f.Name(),
			)
			if err = contentExtractor.IngestRecords(ctx, f); err != nil {
				log.Fatalf("failed to ingest records for %v: %v", inputFileLocation, err)
			}
		}
	} else {
		_, _ = fmt.Fprintln(os.Stderr, "Reading from stdin")
		contentExtractor := kindle.NewContentExtractor(
			model.NewDBBookRepository(db),
			model.NewDBAnnotationRepository(db),
			"stdin",
		)
		if err := contentExtractor.IngestRecords(ctx, os.Stdin); err != nil {
			log.Fatalf("failed to ingest records from standard input: %v", err)
		}
	}
}

func prepareDatabase() *sql.DB {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	return db
}
