package tt_extractor_kindle

import (
	"database/sql"
	"io"
	"log"
	"regexp"
	"strings"
)

type ContentExtractor struct {
	db *sql.DB
}

func NewContentExtractor(db *sql.DB) *ContentExtractor {
	sqlStmt := `
	create table book (
		id integer not null primary key, 
		isbn text,
		name text,
		authors text
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

func (e ContentExtractor) IngestRecords(reader io.Reader) {
	scanner := configureScanner(reader)
	for scanner.Scan() {
		l := scanner.Text()
		log.Println("Encountered line ", l)
		e.ingestAnnotation(l)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

var annotationMetadata = regexp.MustCompile(`\(([^\)]+)\)`)

func (e ContentExtractor) ingestAnnotation(annotation string) {
	rows := strings.Split(annotation, "\n")
	book_metadata := rows[0]
	// TODO: store metadata
	annotation_metadata := rows[1]
	if len(rows) == 2 {
		log.Printf("Ignored empty annotation: %v", annotation_metadata)
		return
	}
	empty_line := rows[2]
	if len(strings.TrimSpace(empty_line)) > 0 {
		log.Fatalf("Expected empty line but encountered: '%v'", empty_line)
	}
	// TODO: store data
	//annotation_rows := rows[3:]

	parenthesesBlocks := annotationMetadata.FindAllStringSubmatch(book_metadata, -1)
	// there might be multiple blocks in parentheses, we want the last one only
	if len(parenthesesBlocks) == 0 {
		log.Fatalf("Expected at least one parenthesesBlocks in line '%v'", empty_line)
	}
	author := parenthesesBlocks[len(parenthesesBlocks)-1][1]
	bookName := book_metadata[0 : strings.LastIndex(book_metadata, "(")-1]

	metadata := annotationMetadata.FindAllStringSubmatch(annotation, -1)
	if len(metadata) == 0 {
		log.Fatalf("Failed to match annotation metadata in: %v", annotation)
	}

	bookId := e.upsertBook(bookName, author)
	log.Printf("Book %v", bookId)
}

func (e ContentExtractor) upsertBook(bookName, authors string) (bookId int64) {
	rows, err := e.db.Query("select id from book where name=? and authors=?", bookName, authors)
	check(err)
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&bookId)
		check(err)
		err = rows.Err()
		check(err)
	} else {
		tx, err := e.db.Begin()
		check(err)
		stmt, err := tx.Prepare("insert into book(isbn, name, authors) values(?,?,?)")
		check(err)
		defer stmt.Close()
		insertResult, err := stmt.Exec("", bookName, authors)
		check(err)
		tx.Commit()
		bookId, err = insertResult.LastInsertId()
	}
	return
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
