package tt_extractor_kindle

import (
	"database/sql"
	log "github.com/sirupsen/logrus"
	"time"
)

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

type Location struct {
	PageStart     *int `json:"pageStart,omitempty"`
	PageEnd       *int `json:"pageEnd,omitempty"`
	LocationStart *int `json:"locationStart,omitempty"`
	LocationEnd   *int `json:"locationEnd,omitempty"`
}

type AnnotationRepository interface {
	upsertAnnotation(a *Annotation) (existed bool)
}

type annotationRepository struct {
	db *sql.DB
}

func NewAnnotationRepository(db *sql.DB) AnnotationRepository {
	//TODO: can location have type json?
	sqlStmt := `
	create table if not exists annotation (
		id integer not null primary key,
		book_id integer, 
		text text,
		location text,
		ts timestamp,
		origin text,
		type text
	);
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}
	return &annotationRepository{
		db: db,
	}
}

func (r *annotationRepository) upsertAnnotation(a *Annotation) (existed bool) {
	tx, err := r.db.Begin()
	check(err)
	if r.findExisting(a) {
		stmt, err := tx.Prepare("update annotation set location=?, text=?, ts=?, origin=?, type=? where id=?")
		check(err)
		defer stmt.Close()
		_, err = stmt.Exec(a.location, a.text, a.ts, a.origin, a.type_, a.id)
		check(err)
		log.Debugf("Updated existing annotation with id %v", a.id)
		existed = true
	} else {
		stmt, err := tx.Prepare("insert into annotation(book_id, location, text, ts, origin, type) values(?,?,?,?,?,?)")
		check(err)
		defer stmt.Close()
		insertResult, err := stmt.Exec(a.bookId, a.location, a.text, a.ts, a.origin, a.type_)
		check(err)
		annotationId, err := insertResult.LastInsertId()
		check(err)
		a.id = annotationId
		log.Debugf("Inserted new annotation with id %v", a.id)
		existed = false
	}
	tx.Commit()
	return
}

func (r *annotationRepository) findExisting(annotation *Annotation) bool {
	rows, err := r.db.Query("select id from annotation where book_id=? and text=?", annotation.bookId, annotation.text)
	check(err)
	defer rows.Close()
	if rows.Next() {
		err := rows.Scan(&annotation.id)
		check(err)
		return true
	}
	return false
}
