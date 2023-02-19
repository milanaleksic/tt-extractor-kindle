package model

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type CachedBookRepository struct {
	knownBooks map[string]Book
	delegate   BookRepository
}

func NewCachedBookRepository(delegate BookRepository) BookRepository {
	return &CachedBookRepository{
		knownBooks: make(map[string]Book),
		delegate:   delegate,
	}
}

func (c *CachedBookRepository) UpsertBook(ctx context.Context, book *Book) (bool, error) {
	if cachedBook, ok := c.knownBooks[c.Hash(*book)]; ok {
		log.Debugf("Skipping book update for %v", book)
		book.Id = cachedBook.Id
		book.Name = cachedBook.Name
		book.Isbn = cachedBook.Isbn
		book.Authors = cachedBook.Authors
		return true, nil
	}
	existed, err := c.delegate.UpsertBook(ctx, book)
	if err == nil {
		c.knownBooks[c.Hash(*book)] = *book
	}
	return existed, err
}

func (c *CachedBookRepository) Hash(b Book) string {
	return fmt.Sprintf("%s/%s", b.Isbn, b.Name)
}
