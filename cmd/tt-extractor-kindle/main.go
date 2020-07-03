package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	extractor "github.com/milanaleksic/tt-extractor-kindle"
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
	var scanner *bufio.Scanner
	if inputFile != nil {
		scanner = bufio.NewScanner(inputFile)
	} else {
		scanner = bufio.NewScanner(os.Stdin)
		_, _ = fmt.Fprintln(os.Stderr, "Reading from stdin")
	}
	configureScanner(scanner)

	_ = os.Remove(databaseLocation)
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", databaseLocation))
	if err != nil {
		log.Fatalf("Failed to open database file: %s, reason: %v", databaseLocation, err)
	}
	extractor.
		NewContentExtractor(db).
		IngestRecords(scanner)
	defer db.Close()
}

func configureScanner(scanner *bufio.Scanner) {
	const MAX_BLOCK_SIZE = 1024 * 1024
	const BUFF_SIZE = 64 * 1024
	const KINDLE_SPLITTER = "=========="

	buf := make([]byte, 0, BUFF_SIZE)
	scanner.Buffer(buf, MAX_BLOCK_SIZE)

	separator := []byte(KINDLE_SPLITTER)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, separator); i >= 0 {
			nbs := skipWhitespace(data, i+1+len(separator)) // next block start
			cbs := skipWhitespace(data, 0)                  // current block start
			cbe := beforeWhitespace(data, i)                // current block ending + 1
			return nbs, data[cbs:cbe], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	})
}

func beforeWhitespace(data []byte, startFrom int) int {
	iter := startFrom
	for iter > 0 && (data[iter-1] == '\n' || data[iter-1] == '\r') {
		iter--
	}
	return iter
}

func skipWhitespace(data []byte, startFrom int) int {
	iter := startFrom
	for {
		if iter < len(data) && (data[iter] == '\n' || data[iter] == '\r') {
			iter++
		} else if iter < len(data)-2 && bytes.Equal(data[iter:iter+3], []byte{0xEF, 0xBB, 0xBF}) {
			iter += 3
		}
		break
	}
	return iter
}
