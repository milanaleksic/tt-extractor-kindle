package model

import (
	"database/sql"
	"github.com/milanaleksic/tt-extractor-kindle/utils"
	log "github.com/sirupsen/logrus"
)

type Book struct {
	Id      int64
	Name    string
	Authors string
	Isbn    string
}

type BookRepository interface {
	UpsertBook(book *Book) (existed bool)
}
type bookRepository struct {
	db *sql.DB
}

func NewBookRepository(db *sql.DB) BookRepository {
	sqlStmt := `
	create table if not exists book (
		Id integer not null primary key, 
		isbn text,
		name text,
		authors text
	);
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}
	return &bookRepository{
		db: db,
	}
}

func (r *bookRepository) UpsertBook(book *Book) (existed bool) {
	tx, err := r.db.Begin()
	utils.Check(err)
	if r.findExisting(book) {
		stmt, err := tx.Prepare("update book set isbn=?, name=?, authors=? where Id=?")
		utils.Check(err)
		defer stmt.Close()
		_, err = stmt.Exec(book.Isbn, book.Name, book.Authors, book.Id)
		utils.Check(err)
		log.Debugf("Updated existing annotation with Id %v", book.Id)
		existed = true
	} else {
		stmt, err := tx.Prepare("insert into book(isbn, name, authors) values(?,?,?)")
		utils.Check(err)
		defer stmt.Close()
		insertResult, err := stmt.Exec(book.Isbn, book.Name, book.Authors)
		utils.Check(err)
		bookId, err := insertResult.LastInsertId()
		book.Id = bookId
		existed = false
	}
	tx.Commit()
	return
}

func (r *bookRepository) findExisting(book *Book) bool {
	rows, err := r.db.Query("select Id from book where name=?", book.Name)
	utils.Check(err)
	defer rows.Close()
	if rows.Next() {
		err := rows.Scan(&book.Id)
		utils.Check(err)
		return true
	}
	return false
}
