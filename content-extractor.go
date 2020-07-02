package tt_extractor_kindle

import (
	"bufio"
	"database/sql"
	"log"
)

type ContentExtractor struct {
	db *sql.DB
}

func NewContentExtractor(db *sql.DB) *ContentExtractor {
	sqlStmt := `
	create table foo (
		id integer not null primary key, 
		line text
	);
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}
	return &ContentExtractor{
		db: db,
	}
}

func (e ContentExtractor) IngestRecords(scanner *bufio.Scanner) {
	for scanner.Scan() {
		l := scanner.Text()
		log.Println("Encountered line ", l)
		e.ingestRow(l)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (e ContentExtractor) ingestRow(l string) {
	tx, err := e.db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("insert into foo(line) values(?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(l)
	if err != nil {
		log.Fatal(err)
	}
	tx.Commit()
}
