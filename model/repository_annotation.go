package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
	Location Location
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

func (l Location) IsEmpty() bool {
	return l.PageStart == nil && l.PageEnd == nil &&
		l.LocationStart == nil && l.LocationEnd == nil
}

type AnnotationRepository interface {
	UpsertAnnotation(ctx context.Context, a *Annotation) (existed bool, err error)
}

type annotationRepository struct {
	db *sql.DB
}

func NewDBAnnotationRepository(db *sql.DB) AnnotationRepository {
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
    create index if not exists annotation_text on annotation(book_id, text);
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}
	return &annotationRepository{
		db: db,
	}
}

func (r *annotationRepository) UpsertAnnotation(ctx context.Context, a *Annotation) (existed bool, err error) {
	tx, err := r.db.Begin()
	utils.MustCheck(err)
	existingA, ok, err := r.findByBookIdAndText(a.BookId, a.Text)
	if err != nil {
		return false, fmt.Errorf("failed to upsert annotation: %w", err)
	}
	if ok {
		a.Id = existingA.Id
		if !existingA.Location.IsEmpty() && !a.Location.IsEmpty() {
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
		utils.MustCheck(err)
		defer utils.SafeClose(stmt, &err)
		locationAsString, err := json.Marshal(a.Location)
		if err != nil {
			log.Fatalf("unexpected problem: could not serialize into JSON %+v: %v", a.Location, err)
		}
		utils.MustCheck(err)
		_, err = stmt.Exec(locationAsString, a.Text, a.Ts, a.Origin, a.Type, a.Id)
		if err != nil {
			return false, fmt.Errorf("failed to update existing annotation: %w", err)
		}
		log.Debugf("Updated existing annotation with Id %v", a.Id)
		existed = true
	} else {
		stmt, err := tx.Prepare("insert into annotation(book_id, location, text, ts, origin, type) values(?,?,?,?,?,?)")
		utils.MustCheck(err)
		defer utils.SafeClose(stmt, &err)
		locationAsString, err := json.Marshal(a.Location)
		if err != nil {
			log.Fatalf("unexpected problem: could not serialize into JSON %+v: %v", a.Location, err)
		}
		utils.MustCheck(err)
		insertResult, err := stmt.Exec(a.BookId, locationAsString, a.Text, a.Ts, a.Origin, a.Type)
		if err != nil {
			return false, fmt.Errorf("failed to insert new annotation: %w", err)
		}
		annotationId, err := insertResult.LastInsertId()
		if err != nil {
			return false, fmt.Errorf("failed to retrieve last inserted annotation: %w", err)
		}
		a.Id = annotationId
		log.Debugf("Inserted new annotation with Id %v", a.Id)
		existed = false
	}
	utils.MustCheck(tx.Commit())
	return
}

func (r *annotationRepository) findByBookIdAndText(bookId int64, text string) (a *Annotation, ok bool, err error) {
	rows, err := r.db.Query("select Id, book_id, location, text, ts, origin, type from annotation where book_id=? and text=?", bookId, text)
	if err != nil {
		return nil, false, fmt.Errorf("failed to run the query findByBookIdAndText: %w", err)
	}
	defer utils.SafeClose(rows, &err)
	a = &Annotation{}
	var locationAsString string
	if rows.Next() {
		err := rows.Scan(&a.Id, &a.BookId, &locationAsString, &a.Text, &a.Ts, &a.Origin, &a.Type)
		if err != nil {
			return nil, false, fmt.Errorf("failed to scan successfully retrieved result set for annotation: %w", err)
		}
		if locationAsString != "" {
			err = json.Unmarshal([]byte(locationAsString), &a.Location)
			if err != nil {
				log.Fatalf("unexpected problem: could not deserialize into JSON %+v: %v", a.Location, err)
			}
		}
		utils.MustCheck(err)
		return a, true, nil
	}
	return nil, false, nil
}
