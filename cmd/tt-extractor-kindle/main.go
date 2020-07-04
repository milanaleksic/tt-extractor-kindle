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
	flag.Var(&inputFileLocations, "input-file", "input clipping files")
	flag.StringVar(&databaseLocation, "database", "clippings.db", "SQLite3 database location")
	flag.Parse()

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
	defer db.Close()

	var openedFiles []io.Closer
	defer func() {
		for _, f := range openedFiles {
			err := f.Close()
			if err != nil {
				log.Printf("Failed to close file %v, err=%v", f, err)
			}
		}
	}()
	contentExtractor := extractor.NewContentExtractor(db)
	if len(inputFileLocations) > 0 {
		for _, inputFileLocation := range inputFileLocations {
			f, err := os.Open(inputFileLocation)
			if err != nil {
				log.Fatalf("Failed to open input file: %s, reason: %v", inputFileLocation, err)
			}
			openedFiles = append(openedFiles, f)
			contentExtractor.IngestRecords(f, f.Name())
		}
	} else {
		_, _ = fmt.Fprintln(os.Stderr, "Reading from stdin")
		contentExtractor.IngestRecords(os.Stdin, "stdin")
	}
}

func prepareDatabase() *sql.DB {
	_ = os.Remove(databaseLocation)
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	return db
}
