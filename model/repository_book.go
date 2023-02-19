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

func (b *Book) hash() string {
	return fmt.Sprintf("%s/%s", b.Isbn, b.Name)
}

type BookRepository interface {
	UpsertBook(ctx context.Context, book *Book) (existed bool, err error)
}
type bookRepository struct {
	db         *sql.DB
	knownBooks map[string]Book
}

func NewDBBookRepository(db *sql.DB) BookRepository {
	sqlStmt := `
	create table if not exists book (
		Id integer not null primary key, 
		isbn text,
		name text,
		authors text
	);
    create index if not exists book_name on book(name);
	create index if not exists book_isbn_name on book(isbn);
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}
	return &bookRepository{
		db:         db,
		knownBooks: make(map[string]Book),
	}
}

func (r *bookRepository) UpsertBook(ctx context.Context, book *Book) (existed bool, err error) {
	if cachedBook, ok := r.knownBooks[book.hash()]; ok {
		log.Debugf("Skipping book update for %v", book)
		book.Id = cachedBook.Id
		book.Name = cachedBook.Name
		book.Isbn = cachedBook.Isbn
		book.Authors = cachedBook.Authors
		return true, nil
	}
	tx, err := r.db.Begin()
	utils.MustCheck(err)
	existingBook, err := r.find(book)
	if err != nil {
		return false, fmt.Errorf("failed to upsert book: %w", err)
	}
	if existingBook != nil {
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
		log.Debugf("Updated existing book with Id %v", book.Id)
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
	r.knownBooks[book.hash()] = *book
	return
}

func (r *bookRepository) find(bookTemplate *Book) (book *Book, err error) {
	book = &Book{}
	if bookTemplate.Isbn != "" {
		row := r.db.QueryRow("select book.id, book.name, book.isbn, book.authors from book where isbn=?", bookTemplate.Isbn)
		err = row.Scan(&book.Id, &book.Name, &book.Isbn, &book.Authors)
		if err == nil {
			return book, nil
		}
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to scan successfully retrieved result set for book: %w", err)
		}
	}

	row := r.db.QueryRow("select book.id, book.name, book.isbn, book.authors from book where name=?", bookTemplate.Name)
	err = row.Scan(&book.Id, &book.Name, &book.Isbn, &book.Authors)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to scan successfully retrieved result set for book: %w", err)
	}
	return book, nil
}
