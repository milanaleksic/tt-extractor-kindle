package model

import (
	"context"
	"database/sql"
	"fmt"
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
	UpsertBook(ctx context.Context, book *Book) (existed bool, err error)
}
type bookRepository struct {
	db *sql.DB
}

func NewDBBookRepository(db *sql.DB) BookRepository {
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

func (r *bookRepository) UpsertBook(ctx context.Context, book *Book) (existed bool, err error) {
	tx, err := r.db.Begin()
	utils.MustCheck(err)
	existingBook, ok, err := r.findByName(book.Name)
	if err != nil {
		return false, fmt.Errorf("failed to upsert book: %w", err)
	}
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
		utils.MustCheck(err)
		defer utils.SafeClose(stmt, &err)
		_, err = stmt.Exec(book.Isbn, book.Name, book.Authors, book.Id)
		if err != nil {
			return false, fmt.Errorf("failed to update existing book: %w", err)
		}
		log.Debugf("Updated existing annotation with Id %v", book.Id)
		existed = true
	} else {
		stmt, err := tx.Prepare("insert into book(isbn, name, authors) values(?,?,?)")
		utils.MustCheck(err)
		defer utils.SafeClose(stmt, &err)
		insertResult, err := stmt.Exec(book.Isbn, book.Name, book.Authors)
		if err != nil {
			return false, fmt.Errorf("failed to retrieve last inserted book: %w", err)
		}
		bookId, err := insertResult.LastInsertId()
		book.Id = bookId
		existed = false
	}
	utils.MustCheck(tx.Commit())
	return
}

func (r *bookRepository) findByName(name string) (book *Book, ok bool, err error) {
	rows, err := r.db.Query("select book.id, book.name, book.isbn, book.authors from book where name=?", name)
	if err != nil {
		return nil, false, fmt.Errorf("failed to run the query findByName: %w", err)
	}
	defer utils.SafeClose(rows, &err)
	book = &Book{}
	if rows.Next() {
		err := rows.Scan(&book.Id, &book.Name, &book.Isbn, &book.Authors)
		if err != nil {
			return nil, false, fmt.Errorf("failed to scan successfully retrieved result set for book: %w", err)
		}
		return book, true, nil
	}
	return nil, false, nil
}
