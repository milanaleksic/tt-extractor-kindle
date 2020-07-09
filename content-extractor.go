package tt_extractor_kindle

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io"
	"regexp"
	"strings"
	"time"
)

type ContentExtractor struct {
	bookRepo            BookRepository
	annotationRepo      AnnotationRepository
	annotationsUpdated  int
	annotationsInserted int
}

func NewContentExtractor(bookRepo BookRepository, annotationRepo AnnotationRepository) *ContentExtractor {
	return &ContentExtractor{
		bookRepo:       bookRepo,
		annotationRepo: annotationRepo,
	}
}

func (e *ContentExtractor) IngestRecords(reader io.Reader, origin string) {
	begin := time.Now()
	scanner := configureScanner(reader)
	for scanner.Scan() {
		l := scanner.Text()
		log.Debugf("Encountered line ", l)
		e.ingestAnnotation(l, origin)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Infof("Ingestion completed from origin %v in %dms; updated %v annotations and created %v new ones",
		origin, time.Now().Sub(begin).Milliseconds(), e.annotationsUpdated, e.annotationsInserted)
}

var bookMetadataRegex = regexp.MustCompile(`\(([^\)]+)\)`)

func (e *ContentExtractor) ingestAnnotation(annotation string, origin string) {
	rows := strings.Split(annotation, "\n")
	if len(rows) == 2 {
		log.Debugf("Ignored empty annotation: %v", annotation)
		return
	}
	emptyLine := rows[2]
	if len(strings.TrimSpace(emptyLine)) > 0 {
		log.Fatalf("Expected empty line but encountered: '%v'", emptyLine)
	}
	// TODO: store data
	bookMetadata := rows[0]
	// TODO: store annotationMetadata
	annotationMetadata := rows[1]
	annotationData := rows[3:]

	e.processAnnotation(bookMetadata, annotationMetadata, annotationData, origin)
}

var annotationMetadataRegex = regexp.MustCompile(`- (?:Your )?(Note|Highlight) (?:(?:Loc.|on Page|on page) (\d+)(?: |-(\d+) )\| )?(?:(?:Location|(?:at )?location) (\d+)(?:-(\d+))? \| )?Added on (.*)`)

var layouts = []string{
	"Monday, January 2, 2006 3:04:05 PM",
	"Monday, January 2, 2006 3:04 PM",
	"Monday, 2 January 06 15:04:05",
}

func (e *ContentExtractor) processAnnotation(bookMetadata string, annotationMetadata string, annotationData []string, origin string) {
	bookId := e.getBookId(bookMetadata)
	annotationMetadataParsed := annotationMetadataRegex.FindAllStringSubmatch(annotationMetadata, -1)
	if len(annotationMetadataParsed) == 0 {
		log.Fatalf("Failed to match annotation regex in: %v", annotationMetadata)
	}
	matched := annotationMetadataParsed[0]
	field := 1
	var type_ annotationType
	switch matched[field] {
	case "Highlight":
		type_ = Highlight
	case "Note":
		type_ = Note
	default:
		log.Fatalf("Not supported type: %v", matched[field])
	}
	field++
	location := Location{
		PageStart:     MustItoa(matched[field]),
		PageEnd:       MustItoa(matched[field+1]),
		LocationStart: MustItoa(matched[field+2]),
		LocationEnd:   MustItoa(matched[field+3]),
	}
	field += 4
	timeMatch := strings.TrimSpace(matched[field])
	field++
	var parsedTime time.Time
	var err error
	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, timeMatch)
		if err == nil {
			break
		}
	}
	locationAsString, err := json.Marshal(location)
	check(err)
	annotation := Annotation{
		id:       0,
		bookId:   bookId,
		text:     strings.Join(annotationData, "\n"),
		location: string(locationAsString),
		ts:       parsedTime,
		origin:   origin,
		type_:    type_,
	}
	if e.annotationRepo.upsertAnnotation(&annotation) {
		e.annotationsUpdated++
	} else {
		e.annotationsInserted++
	}
}

func (e *ContentExtractor) getBookId(bookMetadata string) (bookId int64) {
	parenthesesBlocks := bookMetadataRegex.FindAllStringSubmatch(bookMetadata, -1)
	author := ""
	bookName := bookMetadata
	// if we can match one parenthesis block, probably it is the author name
	if len(parenthesesBlocks) != 0 {
		author = parenthesesBlocks[len(parenthesesBlocks)-1][1]
		bookName = bookMetadata[0 : strings.LastIndex(bookMetadata, "(")-1]
	}
	return e.bookRepo.upsertBook(bookName, author)
}
