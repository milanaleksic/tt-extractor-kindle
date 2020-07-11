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
	existingBook, ok := r.findByName(book.Name)
	if ok {
		book.Id = existingBook.Id
		if existingBook.Isbn != "" && book.Isbn == "" {
			book.Isbn = existingBook.Isbn
		}
		if existingBook.Name != "" && book.Name == "" {
			book.Name = existingBook.Name
		}
		if existingBook.Authors != "" && book.Authors == "" {
			book.Authors = existingBook.Authors
		}
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

func (r *bookRepository) findByName(name string) (book *Book, ok bool) {
	rows, err := r.db.Query("select book.id, book.name, book.isbn, book.authors from book where name=?", name)
	utils.Check(err)
	defer rows.Close()
	book = &Book{}
	if rows.Next() {
		err := rows.Scan(&book.Id, &book.Name, &book.Isbn, &book.Authors)
		utils.Check(err)
		return book, true
	}
	return nil, false
}
