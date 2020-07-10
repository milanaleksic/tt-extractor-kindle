package model

import (
	"database/sql"
	"github.com/milanaleksic/tt-extractor-kindle/utils"
	log "github.com/sirupsen/logrus"
)

type BookRepository interface {
	UpsertBook(bookName, authors string) (bookId int64)
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

func (r *bookRepository) UpsertBook(bookName, authors string) (bookId int64) {
	rows, err := r.db.Query("select Id from book where name=? and authors=?", bookName, authors)
	utils.Check(err)
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&bookId)
		utils.Check(err)
		err = rows.Err()
		utils.Check(err)
	} else {
		tx, err := r.db.Begin()
		utils.Check(err)
		stmt, err := tx.Prepare("insert into book(isbn, name, authors) values(?,?,?)")
		utils.Check(err)
		defer stmt.Close()
		insertResult, err := stmt.Exec("", bookName, authors)
		utils.Check(err)
		tx.Commit()
		bookId, err = insertResult.LastInsertId()
	}
	return
}
