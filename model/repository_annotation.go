package model

import (
	"database/sql"
	"github.com/milanaleksic/tt-extractor-kindle/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

type AnnotationType string

const (
	Note      AnnotationType = "note"
	Highlight AnnotationType = "highlight"
)

type Annotation struct {
	Id       int64
	BookId   int64
	Text     string
	Location string
	Ts       time.Time
	Origin   string
	Type     AnnotationType
}

type Location struct {
	PageStart     *int `json:"pageStart,omitempty"`
	PageEnd       *int `json:"pageEnd,omitempty"`
	LocationStart *int `json:"locationStart,omitempty"`
	LocationEnd   *int `json:"locationEnd,omitempty"`
}

type AnnotationRepository interface {
	UpsertAnnotation(a *Annotation) (existed bool)
}

type annotationRepository struct {
	db *sql.DB
}

func NewAnnotationRepository(db *sql.DB) AnnotationRepository {
	//TODO: can location have type json?
	sqlStmt := `
	create table if not exists annotation (
		Id integer not null primary key,
		book_id integer, 
		text text,
		location text,
		ts timestamp,
		origin text,
		type text,
    FOREIGN KEY (book_id)
       REFERENCES book (id) 
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

func (r *annotationRepository) UpsertAnnotation(a *Annotation) (existed bool) {
	tx, err := r.db.Begin()
	utils.Check(err)
	existingA, ok := r.findByBookIdAndText(a.BookId, a.Text)
	if ok {
		a.Id = existingA.Id
		if existingA.Location != "" && a.Location == "" {
			a.Location = existingA.Location
		}
		if existingA.Text != "" && a.Text == "" {
			a.Text = existingA.Text
		}
		if existingA.Ts.Unix() != 0 && a.Ts.Unix() == 0 {
			a.Ts = existingA.Ts
		}
		if existingA.Origin != "" && a.Origin == "" {
			a.Origin = existingA.Origin
		}
		if existingA.Type != "" && a.Type == "" {
			a.Type = existingA.Type
		}
		stmt, err := tx.Prepare("update annotation set location=?, text=?, ts=?, origin=?, type=? where Id=?")
		utils.Check(err)
		defer stmt.Close()
		_, err = stmt.Exec(a.Location, a.Text, a.Ts, a.Origin, a.Type, a.Id)
		utils.Check(err)
		log.Debugf("Updated existing annotation with Id %v", a.Id)
		existed = true
	} else {
		stmt, err := tx.Prepare("insert into annotation(book_id, location, text, ts, origin, type) values(?,?,?,?,?,?)")
		utils.Check(err)
		defer stmt.Close()
		insertResult, err := stmt.Exec(a.BookId, a.Location, a.Text, a.Ts, a.Origin, a.Type)
		utils.Check(err)
		annotationId, err := insertResult.LastInsertId()
		utils.Check(err)
		a.Id = annotationId
		log.Debugf("Inserted new annotation with Id %v", a.Id)
		existed = false
	}
	tx.Commit()
	return
}

func (r *annotationRepository) findByBookIdAndText(bookId int64, text string) (a *Annotation, ok bool) {
	rows, err := r.db.Query("select Id, book_id, location, text, ts, origin, type from annotation where book_id=? and text=?", bookId, text)
	utils.Check(err)
	defer rows.Close()
	a = &Annotation{}
	if rows.Next() {
		err := rows.Scan(&a.Id, &a.BookId, &a.Location, &a.Text, &a.Ts, &a.Origin, &a.Type)
		utils.Check(err)
		return a, true
	}
	return nil, false
}
