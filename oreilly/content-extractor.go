package oreilly

import (
	"encoding/csv"
	"fmt"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	log "github.com/sirupsen/logrus"
	"io"
	"reflect"
	"regexp"
	"time"
)

var (
	isbnRegex          = regexp.MustCompile(`(?:97[89])?\d{9}(?:\d|X)`)
	supportedCsvFormat = []string{
		"Book Title",
		"Authors",
		"Chapter Title",
		"Date of Highlight",
		"Book URL",
		"Chapter URL",
		"Highlight URL",
		"Highlight",
		"Personal Note",
	}
)

type ContentExtractor struct {
	bookRepo            model.BookRepository
	annotationRepo      model.AnnotationRepository
	annotationsUpdated  int
	annotationsInserted int
}

func NewContentExtractor(bookRepo model.BookRepository, annotationRepo model.AnnotationRepository) *ContentExtractor {
	return &ContentExtractor{
		bookRepo:       bookRepo,
		annotationRepo: annotationRepo,
	}
}

func (e *ContentExtractor) IngestRecords(reader io.Reader) (err error) {
	begin := time.Now()
	r := csv.NewReader(reader)
	firstRecord := true
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if firstRecord {
			if !reflect.DeepEqual(record, supportedCsvFormat) {
				log.Fatalf("CSV does not have expected format: %+v encountered, but expected: %v", record, supportedCsvFormat)
			}
			firstRecord = false
		} else {
			err := e.ingestRecord(record)
			if err != nil {
				return fmt.Errorf("error while ingesting row %+v: %w", record, err)
			}
		}
	}
	log.Infof("Ingestion completed from oreilly in %dms; updated %v annotations and created %v new ones",
		time.Now().Sub(begin).Milliseconds(), e.annotationsUpdated, e.annotationsInserted)
	return err
}

func (e *ContentExtractor) ingestRecord(record []string) (err error) {
	book := &model.Book{
		Name:    record[0],
		Authors: record[1],
		Isbn:    "",
	}

	submatch := isbnRegex.FindAllStringSubmatch(record[4], -1)
	if len(submatch) == 0 {
		return fmt.Errorf("could not match the ISBN in the book page URL: %v", record[4])
	}
	book.Isbn = submatch[0][0]

	_, err = e.bookRepo.UpsertBook(book)
	if err != nil {
		log.Errorf("Failed to upsert a book: %v", err)
		return
	}

	parsedTime, err := time.Parse("2006-01-02", record[3])
	if err != nil {
		return fmt.Errorf("could not parsedTime the day of highlight from %v: %w", record[3], err)
	}
	a := &model.Annotation{
		BookId:   book.Id,
		Text:     record[7],
		Location: "",
		Ts:       parsedTime,
		Origin:   record[4],
		Type:     model.Highlight,
	}
	existed, err := e.annotationRepo.UpsertAnnotation(a)
	if err != nil {
		log.Errorf("Failed to upsert an annotation: %v", err)
		return
	}
	if existed {
		e.annotationsUpdated++
	} else {
		e.annotationsInserted++
	}

	return
}
