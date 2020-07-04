package tt_extractor_kindle

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ContentExtractor struct {
	db *sql.DB
}

func NewContentExtractor(db *sql.DB) *ContentExtractor {
	//TODO: can location have type json?
	sqlStmt := `
	create table book (
		id integer not null primary key, 
		isbn text,
		name text,
		authors text
	);
	create table annotation (
		id integer not null primary key,
		book_id integer, 
		text text,
		location text,
		ts timestamp
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

var bookMetadataRegex = regexp.MustCompile(`\(([^\)]+)\)`)

func (e ContentExtractor) ingestAnnotation(annotation string) {
	rows := strings.Split(annotation, "\n")
	if len(rows) == 2 {
		log.Printf("Ignored empty annotation: %v", annotation)
		return
	}
	emptyLine := rows[2]
	if len(strings.TrimSpace(emptyLine)) > 0 {
		log.Fatalf("Expected empty line but encountered: '%v'", emptyLine)
	}
	// TODO: store data
	bookMetadata := rows[0]
	// TODO: store annotationMetadata
	annotationMetadata := rows[1]
	annotationData := rows[3:]

	e.processAnnotation(bookMetadata, annotationMetadata, annotationData)
}

func (e ContentExtractor) getBookId(book_metadata string) (bookId int64) {
	parenthesesBlocks := bookMetadataRegex.FindAllStringSubmatch(book_metadata, -1)
	// there might be multiple blocks in parentheses, we want the last one only
	if len(parenthesesBlocks) == 0 {
		log.Fatalf("Expected at least one parenthesesBlocks in line '%v'", book_metadata)
	}
	author := parenthesesBlocks[len(parenthesesBlocks)-1][1]
	bookName := book_metadata[0 : strings.LastIndex(book_metadata, "(")-1]

	return e.upsertBook(bookName, author)
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

type Location struct {
	PageStart     *int `json:"pageStart,omitempty"`
	PageEnd       *int `json:"pageEnd,omitempty"`
	LocationStart *int `json:"locationStart,omitempty"`
	LocationEnd   *int `json:"locationEnd,omitempty"`
}

var annotationMetadataRegex = regexp.MustCompile(`- (?:Your )?Highlight (?:(?:Loc.|on Page) (\d+)(?: |-(\d+) )\| )?(?:Location (\d+)(?: |-(\d+)) \| )?Added on ((?:[a-zA-Z]+), [a-zA-Z]+ \d+, \d+ \d+:\d+(?::\d+)? (?:AM|PM))`)

const layoutWithSeconds = "Monday, January 2, 2006 3:04:05 PM"
const layoutWithoutSeconds = "Monday, January 2, 2006 3:04 PM"

func (e ContentExtractor) processAnnotation(bookMetadata string, annotationMetadata string, annotationData []string) {
	bookId := e.getBookId(bookMetadata)

	annotationMetadataParsed := annotationMetadataRegex.FindAllStringSubmatch(annotationMetadata, -1)
	if len(annotationMetadataParsed) == 0 {
		log.Fatalf("Failed to match annotation annotationMetadata in: %v", annotationMetadata)
	}
	matched := annotationMetadataParsed[0]
	location := Location{
		PageStart:     MustItoa(matched[1]),
		PageEnd:       MustItoa(matched[2]),
		LocationStart: MustItoa(matched[3]),
		LocationEnd:   MustItoa(matched[4]),
	}
	timeMatch := matched[5]
	parse, err := time.Parse(layoutWithSeconds, timeMatch)
	if err != nil {
		parse, err = time.Parse(layoutWithoutSeconds, timeMatch)
		check(err)
	}
	locationAsString, err := json.Marshal(location)
	check(err)
	annotation := Annotation{
		id:       0,
		bookId:   bookId,
		text:     strings.Join(annotationData, "\n"),
		location: string(locationAsString),
		ts:       parse,
	}
	e.upsertAnnotation(&annotation)
}

type Annotation struct {
	id       int64
	bookId   int64
	text     string
	location string
	ts       time.Time
}

func (e ContentExtractor) upsertAnnotation(annotation *Annotation) {
	rows, err := e.db.Query("select id from annotation where book_id=? and text=?", annotation.bookId, annotation.text)
	check(err)
	defer rows.Close()
	tx, err := e.db.Begin()
	check(err)
	if rows.Next() {
		err := rows.Scan(&annotation.id)
		check(err)
		stmt, err := tx.Prepare("update annotation set location=?, text=?, ts=? where id=?")
		check(err)
		defer stmt.Close()
		_, err = stmt.Exec(annotation.location, annotation.text, annotation.ts, annotation.id)
		check(err)
	} else {
		stmt, err := tx.Prepare("insert into annotation(book_id, location, text, ts) values(?,?,?,?)")
		check(err)
		defer stmt.Close()
		insertResult, err := stmt.Exec(annotation.bookId, annotation.location, annotation.text, annotation.ts)
		check(err)
		annotationId, err := insertResult.LastInsertId()
		check(err)
		annotation.id = annotationId
	}
	tx.Commit()
	return
}

func MustItoa(s string) *int {
	if s == "" {
		return nil
	}
	result, err := strconv.Atoi(s)
	check(err)
	return &result
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
