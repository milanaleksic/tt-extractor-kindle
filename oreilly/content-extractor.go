package oreilly

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	log "github.com/sirupsen/logrus"
	"io"
	"reflect"
	"regexp"
	"time"
)

type format byte

const (
	formatUnknown format = iota
	formatV1
	formatV2
)

var (
	isbnRegex = regexp.MustCompile(`(?:97[89])?\d{9}(?:\d|X)`)
	formats   = map[format][]string{
		formatV1: {
			"Book Title",
			"Authors",
			"Chapter Title",
			"Date of Highlight",
			"Book URL",
			"Chapter URL",
			"Highlight URL",
			"Highlight",
			"Personal Note",
		},
		formatV2: {
			"Book Title",
			"Chapter Title",
			"Date of Highlight",
			"Book URL",
			"Chapter URL",
			"Annotation URL",
			"Highlight",
			"Personal Note",
		},
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
		bookRepo:       model.NewCachedBookRepository(bookRepo),
		annotationRepo: annotationRepo,
	}
}

func (e *ContentExtractor) IngestRecords(ctx context.Context, reader io.Reader) (err error) {
	begin := time.Now()
	r := csv.NewReader(reader)
	firstRecord := true
	version := formatUnknown
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if firstRecord {
			if reflect.DeepEqual(record, formats[formatV1]) {
				version = formatV1
			} else if reflect.DeepEqual(record, formats[formatV2]) {
				version = formatV2
			} else {
				return fmt.Errorf("CSV does not have expected format: %+v encountered, but expected one of: %+v", record, formats)
			}
			log.Infof("Proceeding with format version %v", version)
			firstRecord = false
		} else {
			if version == formatV1 {
				err = e.ingestRecordV1(ctx, record)
			} else if version == formatV2 {
				err = e.ingestRecordV2(ctx, record)
			}
			if err != nil {
				return fmt.Errorf("error while ingesting row %+v: %w", record, err)
			}
		}
	}
	log.Infof("Ingestion completed from oreilly in %dms; updated %v annotations and created %v new ones",
		time.Now().Sub(begin).Milliseconds(), e.annotationsUpdated, e.annotationsInserted)
	return err
}

func (e *ContentExtractor) ingestRecordV1(ctx context.Context, record []string) (err error) {
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

	_, err = e.bookRepo.UpsertBook(ctx, book)
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
		Location: model.Location{},
		Ts:       parsedTime,
		Origin:   record[4],
		Type:     model.Highlight,
	}
	existed, err := e.annotationRepo.UpsertAnnotation(ctx, a)
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

func (e *ContentExtractor) ingestRecordV2(ctx context.Context, record []string) (err error) {
	book := &model.Book{
		Name:    record[0],
		Authors: "",
		Isbn:    "",
	}

	submatch := isbnRegex.FindAllStringSubmatch(record[3], -1)
	if len(submatch) == 0 {
		return fmt.Errorf("could not match the ISBN in the book page URL: %v", record[3])
	}
	book.Isbn = submatch[0][0]

	_, err = e.bookRepo.UpsertBook(ctx, book)
	if err != nil {
		log.Errorf("Failed to upsert a book: %v", err)
		return
	}

	parsedTime, err := time.Parse("2006-01-02", record[2])
	if err != nil {
		return fmt.Errorf("could not parsedTime the day of highlight from %v: %w", record[2], err)
	}
	a := &model.Annotation{
		BookId:   book.Id,
		Text:     record[6],
		Location: model.Location{},
		Ts:       parsedTime,
		Origin:   record[3],
		Type:     model.Highlight,
	}
	existed, err := e.annotationRepo.UpsertAnnotation(ctx, a)
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
