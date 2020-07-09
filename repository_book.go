package tt_extractor_kindle

import (
	"database/sql"
	log "github.com/sirupsen/logrus"
)

type BookRepository interface {
	upsertBook(bookName, authors string) (bookId int64)
}
type bookRepository struct {
	db *sql.DB
}

func NewBookRepository(db *sql.DB) BookRepository {
	sqlStmt := `
	create table if not exists book (
		id integer not null primary key, 
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

func (r *bookRepository) upsertBook(bookName, authors string) (bookId int64) {
	rows, err := r.db.Query("select id from book where name=? and authors=?", bookName, authors)
	check(err)
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&bookId)
		check(err)
		err = rows.Err()
		check(err)
	} else {
		tx, err := r.db.Begin()
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
