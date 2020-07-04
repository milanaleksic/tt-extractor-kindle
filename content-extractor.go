package tt_extractor_kindle

import (
	"database/sql"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io"
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
		ts timestamp,
		origin text
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

func (e ContentExtractor) IngestRecords(reader io.Reader, origin string) {
	scanner := configureScanner(reader)
	for scanner.Scan() {
		l := scanner.Text()
		log.Debugf("Encountered line ", l)
		e.ingestAnnotation(l, origin)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

var bookMetadataRegex = regexp.MustCompile(`\(([^\)]+)\)`)

func (e ContentExtractor) ingestAnnotation(annotation string, origin string) {
	rows := strings.Split(annotation, "\n")
	if len(rows) == 2 {
		log.Debugf("Ignored empty annotation: %v", annotation)
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

	e.processAnnotation(bookMetadata, annotationMetadata, annotationData, origin)
}

func (e ContentExtractor) getBookId(bookMetadata string) (bookId int64) {
	parenthesesBlocks := bookMetadataRegex.FindAllStringSubmatch(bookMetadata, -1)
	author := ""
	bookName := bookMetadata
	// if we can match one parenthesis block, probably it is the author name
	if len(parenthesesBlocks) != 0 {
		author = parenthesesBlocks[len(parenthesesBlocks)-1][1]
		bookName = bookMetadata[0 : strings.LastIndex(bookMetadata, "(")-1]
	}
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

var annotationMetadataRegex = regexp.MustCompile(`- (?:Your )?(Note|Highlight) (?:(?:Loc.|on Page) (\d+)(?: |-(\d+) )\| )?(?:Location (\d+)(?:-(\d+))? \| )?Added on (.*)`)

var layouts = []string{
	"Monday, January 2, 2006 3:04:05 PM",
	"Monday, January 2, 2006 3:04 PM",
	"Monday, 2 January 06 15:04:05",
}

func (e ContentExtractor) processAnnotation(bookMetadata string, annotationMetadata string, annotationData []string, origin string) {
	bookId := e.getBookId(bookMetadata)

	annotationMetadataParsed := annotationMetadataRegex.FindAllStringSubmatch(annotationMetadata, -1)
	if len(annotationMetadataParsed) == 0 {
		log.Fatalf("Failed to match annotation annotationMetadata in: %v", annotationMetadata)
	}
	matched := annotationMetadataParsed[0]
	field := 1
	var type_ annotationType
	switch matched[field] {
	case "Highlight":
		type_ = Highlight
	case "Note":
		type_ = Note
	default:
		log.Fatalf("Not supported type: %v", matched[field])
	}
	field++
	location := Location{
		PageStart:     MustItoa(matched[field]),
		PageEnd:       MustItoa(matched[field+1]),
		LocationStart: MustItoa(matched[field+2]),
		LocationEnd:   MustItoa(matched[field+3]),
	}
	field += 4
	timeMatch := strings.TrimSpace(matched[field])
	field++
	var parsedTime time.Time
	var err error
	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, timeMatch)
		if err == nil {
			break
		}
	}
	locationAsString, err := json.Marshal(location)
	check(err)
	annotation := Annotation{
		id:       0,
		bookId:   bookId,
		text:     strings.Join(annotationData, "\n"),
		location: string(locationAsString),
		ts:       parsedTime,
		origin:   origin,
		type_:    type_,
	}
	e.upsertAnnotation(&annotation)
}

type annotationType string

const (
	Note      annotationType = "note"
	Highlight annotationType = "highlight"
)

type Annotation struct {
	id       int64
	bookId   int64
	text     string
	location string
	ts       time.Time
	origin   string
	type_    annotationType
}

func (e ContentExtractor) upsertAnnotation(a *Annotation) {
	tx, err := e.db.Begin()
	check(err)
	if e.findExisting(a) {
		stmt, err := tx.Prepare("update annotation set location=?, text=?, ts=?, origin=? where id=?")
		check(err)
		defer stmt.Close()
		_, err = stmt.Exec(a.location, a.text, a.ts, a.origin, a.id)
		check(err)
	} else {
		stmt, err := tx.Prepare("insert into annotation(book_id, location, text, ts, origin) values(?,?,?,?,?)")
		check(err)
		defer stmt.Close()
		insertResult, err := stmt.Exec(a.bookId, a.location, a.text, a.ts, a.origin)
		check(err)
		annotationId, err := insertResult.LastInsertId()
		check(err)
		a.id = annotationId
	}
	tx.Commit()
	return
}

func (e ContentExtractor) findExisting(annotation *Annotation) bool {
	rows, err := e.db.Query("select id from annotation where book_id=? and text=?", annotation.bookId, annotation.text)
	check(err)
	defer rows.Close()
	if rows.Next() {
		err := rows.Scan(&annotation.id)
		check(err)
		return true
	}
	return false
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
