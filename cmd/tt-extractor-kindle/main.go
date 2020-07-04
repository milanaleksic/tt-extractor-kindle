package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	extractor "github.com/milanaleksic/tt-extractor-kindle"
	"io"
	"log"
	"os"
)

var (
	inputFile        *os.File
	databaseLocation string
)

func init() {
	var inputFileLocation string
	flag.StringVar(&inputFileLocation, "input-file", "My Clippings.txt", "input file with clippings")
	flag.StringVar(&databaseLocation, "database", "clippings.db", "SQLite3 database location")
	flag.Parse()

	if inputFileLocation != "" {
		var err error
		inputFile, err = os.Open(inputFileLocation)
		if err != nil {
			log.Fatalf("Failed to open input file: %s, reason: %v", inputFileLocation, err)
		}
	}
}

func main() {
	var reader io.ReadCloser
	if inputFile != nil {
		reader = inputFile
	} else {
		_, _ = fmt.Fprintln(os.Stderr, "Reading from stdin")
		reader = os.Stdin
	}
	defer reader.Close()

	_ = os.Remove(databaseLocation)
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	// https://github.com/mattn/go-sqlite3/issues/209
	//db.SetMaxOpenConns(1)
	extractor.
		NewContentExtractor(db).
		IngestRecords(reader)
	defer db.Close()
}
