package kindle

import (
	"context"
	"fmt"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	"github.com/milanaleksic/tt-extractor-kindle/utils"
	log "github.com/sirupsen/logrus"
	"io"
	"regexp"
	"strings"
	"time"
)

var (
	bookMetadataRegex       = regexp.MustCompile(`\(([^\)]+)\)`)
	annotationMetadataRegex = regexp.MustCompile(`- (?:Your )?(Note|Highlight) (?:(?:Loc.|on Page|on page) (\d+)(?: |-(\d+) )\| )?(?:(?:Location|(?:at )?location) (\d+)(?:-(\d+))? \| )?Added on (.*)`)
	layouts                 = []string{
		"Monday, January 2, 2006 3:04:05 PM",
		"Monday, January 2, 2006 3:04 PM",
		"Monday, 2 January 06 15:04:05",
		"Monday, 2 January 2006 15:04:05",
	}
)

type ContentExtractor struct {
	bookRepo            model.BookRepository
	annotationRepo      model.AnnotationRepository
	annotationsUpdated  int
	annotationsInserted int
	origin              string
	knownBooks          map[string]model.Book
}

func NewContentExtractor(bookRepo model.BookRepository, annotationRepo model.AnnotationRepository, origin string) *ContentExtractor {
	return &ContentExtractor{
		bookRepo:       bookRepo,
		annotationRepo: annotationRepo,
		origin:         origin,
		knownBooks:     make(map[string]model.Book),
	}
}
func (e *ContentExtractor) IngestRecords(ctx context.Context, reader io.Reader) (err error) {
	begin := time.Now()
	scanner := configureScanner(reader)
	for scanner.Scan() {
		l := scanner.Text()
		log.Debugf("Encountered line ", l)
		err := e.ingestAnnotation(ctx, l, e.origin)
		if err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	log.Infof("Ingestion completed from origin %v in %dms; updated %v annotations and created %v new ones",
		e.origin, time.Now().Sub(begin).Milliseconds(), e.annotationsUpdated, e.annotationsInserted)
	return nil
}

func (e *ContentExtractor) ingestAnnotation(ctx context.Context, annotation string, origin string) error {
	rows := strings.Split(annotation, "\n")
	if len(rows) == 2 {
		log.Debugf("Ignored empty annotation: %v", annotation)
		return nil
	}
	emptyLine := rows[2]
	if len(strings.TrimSpace(emptyLine)) > 0 {
		return fmt.Errorf("Expected empty line but encountered: '%v'", emptyLine)
	}
	// TODO: store data
	bookMetadata := rows[0]
	// TODO: store annotationMetadata
	annotationMetadata := rows[1]
	annotationData := rows[3:]

	return e.processAnnotation(ctx, bookMetadata, annotationMetadata, annotationData, origin)
}

func (e *ContentExtractor) processAnnotation(ctx context.Context, bookMetadata string, annotationMetadata string, annotationData []string, origin string) error {
	bookId := e.getBookId(ctx, bookMetadata)
	annotationMetadataParsed := annotationMetadataRegex.FindAllStringSubmatch(annotationMetadata, -1)
	if len(annotationMetadataParsed) == 0 {
		return fmt.Errorf("Failed to match annotation regex in: %v", annotationMetadata)
	}
	matched := annotationMetadataParsed[0]
	field := 1
	var type_ model.AnnotationType
	switch matched[field] {
	case "Highlight":
		type_ = model.Highlight
	case "Note":
		type_ = model.Note
	default:
		return fmt.Errorf("Not supported type: %v", matched[field])
	}
	field++
	location := model.Location{
		PageStart:     utils.MustItoa(matched[field]),
		PageEnd:       utils.MustItoa(matched[field+1]),
		LocationStart: utils.MustItoa(matched[field+2]),
		LocationEnd:   utils.MustItoa(matched[field+3]),
	}
	field += 4
	timeMatch := strings.TrimSpace(matched[field])
	field++
	var parsedTime time.Time
	var err error
	for _, layout := range layouts {
		t, err := time.Parse(layout, timeMatch)
		if err == nil {
			parsedTime = t
			break
		}
	}
	if parsedTime.IsZero() {
		return fmt.Errorf("unexpected problem: time layout not supported %+v", timeMatch)
	}
	annotation := model.Annotation{
		Id:       0,
		BookId:   bookId,
		Text:     strings.Join(annotationData, "\n"),
		Location: location,
		Ts:       parsedTime,
		Origin:   origin,
		Type:     type_,
	}
	existed, err := e.annotationRepo.UpsertAnnotation(ctx, &annotation)
	if err != nil {
		log.Errorf("Failed to upsert an annotation: %v", err)
		return nil
	}
	if existed {
		e.annotationsUpdated++
	} else {
		e.annotationsInserted++
	}
	return nil
}

func (e *ContentExtractor) getBookId(ctx context.Context, bookMetadata string) (bookId int64) {
	parenthesesBlocks := bookMetadataRegex.FindAllStringSubmatch(bookMetadata, -1)
	author := ""
	bookName := bookMetadata
	// if we can match one parenthesis block, probably it is the author name
	if len(parenthesesBlocks) != 0 {
		author = parenthesesBlocks[len(parenthesesBlocks)-1][1]
		bookName = bookMetadata[0 : strings.LastIndex(bookMetadata, "(")-1]
	}
	book := &model.Book{
		Name:    bookName,
		Authors: author,
	}
	_, err := e.upsertBook(ctx, book)
	if err != nil {
		log.Errorf("Failed to upsert a book: %v", err)
		return
	}
	return book.Id
}

func (e *ContentExtractor) Hash(b model.Book) string {
	return fmt.Sprintf("%s/%s", b.Isbn, b.Name)
}

func (e *ContentExtractor) upsertBook(ctx context.Context, book *model.Book) (bool, error) {
	if cachedBook, ok := e.knownBooks[e.Hash(*book)]; ok {
		log.Debugf("Skipping book update for %v", book)
		book.Id = cachedBook.Id
		book.Name = cachedBook.Name
		book.Isbn = cachedBook.Isbn
		book.Authors = cachedBook.Authors
		return true, nil
	}
	existed, err := e.bookRepo.UpsertBook(ctx, book)
	if err != nil {
		e.knownBooks[e.Hash(*book)] = *book
	}
	return existed, err
}
