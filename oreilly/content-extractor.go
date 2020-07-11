package oreilly

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const learningPlatformURL = "https://learning.oreilly.com"

var isbnRegex = regexp.MustCompile(`(?:97[89])?\d{9}(?:\d|X)`)

type ContentExtractor struct {
	bookRepo            model.BookRepository
	annotationRepo      model.AnnotationRepository
	annotationsUpdated  int
	annotationsInserted int
	client              *http.Client
}

func NewContentExtractor(bookRepo model.BookRepository, annotationRepo model.AnnotationRepository, cookies map[string]string) (*ContentExtractor, error) {
	jar, err := createCookieJar(cookies)
	if err != nil {
		return nil, fmt.Errorf("failed to create cookie jar: %w", err)
	}
	return &ContentExtractor{
		bookRepo:       bookRepo,
		annotationRepo: annotationRepo,
		client: &http.Client{
			Jar:     jar,
			Timeout: 10 * time.Second,
		},
	}, nil
}

func (e *ContentExtractor) IngestRecords() (err error) {
	begin := time.Now()
	highlightsPageLocation, err := e.getHighlightsPageLocationFromProfile()
	if err != nil {
		return fmt.Errorf("could not get user id: %w", err)
	}
	body, err := e.readPage(highlightsPageLocation)
	if err != nil {
		return fmt.Errorf("failed to read highlights page body from %v: %w", highlightsPageLocation, err)
	}
	log.Debugf("body of highlights page: %s", body)

	books, err := e.processHighlightsPageForBooks(body)
	if err != nil {
		return fmt.Errorf("failed to fetch books from highlights page: %w", err)
	}
	for _, book := range books {
		if err := e.visitBookPage(book); err != nil {
			return fmt.Errorf("failed visiting book %v: %w", book.name, err)
		}
	}

	log.Infof("Ingestion completed from oreilly in %dms; updated %v annotations and created %v new ones",
		time.Now().Sub(begin).Milliseconds(), e.annotationsUpdated, e.annotationsInserted)
	return err
}

type bookLink struct {
	url  string
	name string
}

func (e *ContentExtractor) processHighlightsPageForBooks(body []byte) (books []*bookLink, err error) {
	var bl *bookLink
	z := html.NewTokenizer(bytes.NewReader(body))
loop:
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			if errors.Is(z.Err(), io.EOF) {
				break loop
			}
			log.Warnf("Encountered unexpected error: %v", z.Err())
			return nil, z.Err()
		case html.TextToken:
			if bl != nil {
				bookName := string(z.Text())
				if len(strings.TrimSpace(bookName)) > 0 {
					log.Debugf("Encountered book: %v", bookName)
					bl.name = bookName
					books = append(books, bl)
				}
			}
		case html.StartTagToken:
			name, hasAttr := z.TagName()
			if atom.Lookup(name) != atom.A || !hasAttr {
				continue
			}
			isBookLink := false
			href := ""
			for {
				k, v, more := z.TagAttr()
				vs := string(v)
				switch atom.Lookup(k) {
				case atom.Class:
					if strings.Contains(vs, "t-book-link") && !strings.Contains(vs, "all-books") {
						isBookLink = true
					}
				case atom.Href:
					href = vs
				}
				if !more {
					break
				}
			}
			if isBookLink {
				bl = &bookLink{
					url: href,
				}
			}
		case html.EndTagToken:
			bl = nil
		}
	}
	return books, nil
}

func (e *ContentExtractor) getHighlightsPageLocationFromProfile() (highlightsPage string, err error) {
	body, err := e.readPage(fmt.Sprintf("%s/profile", learningPlatformURL))
	if err != nil {
		return "", err
	}
	urlPattern := regexp.MustCompile(`window.initialStoreData ?= ?([^;]+);`)
	submatch := urlPattern.FindAllSubmatch(body, -1)
	if len(submatch) == 0 {
		log.Debugf("Unable to parse user ID anywhere on the profile page: %v", string(body))
		return "", fmt.Errorf("no match found for user id")
	}
	var storeData map[string]interface{}
	err = json.Unmarshal(submatch[0][1], &storeData)
	if err != nil {
		return "", fmt.Errorf("failed to parse initial store data from %s: %w", submatch[0][1], err)
	}
	navigationAndAnnouncements := storeData["navigationAndAnnouncements"].(map[string]interface{})
	links := navigationAndAnnouncements["links"].(map[string]interface{})
	profileLinks := links["yourprofile"].([]interface{})
	childrenLinks := profileLinks[0].(map[string]interface{})["children"].([]interface{})
	for _, link := range childrenLinks {
		linkMap := link.(map[string]interface{})
		if linkMap["name"] == "Highlights" {
			return fmt.Sprintf("%s%v", learningPlatformURL, linkMap["link"].(string)), nil
		}
	}
	return "", fmt.Errorf("failed to identify URL for highlights page")
}

func (e *ContentExtractor) readPage(pageUrl string) ([]byte, error) {
	response, err := e.client.Get(pageUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %v: %w", pageUrl, err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body from %v: %w", pageUrl, err)
	}
	return body, nil
}

func (e *ContentExtractor) visitBookPage(bl *bookLink) error {
	body, err := e.readPage(fmt.Sprintf("%s%s", learningPlatformURL, bl.url))
	if err != nil {
		return fmt.Errorf("failed to visit book url %v: %w", bl.url, err)
	}
	//TODO: following pages
	book, annotations, err := e.processHighlightsForBook(body)
	if err != nil {
		return fmt.Errorf("failed to extract book and annotations from the body of the book")
	}
	existed := e.bookRepo.UpsertBook(book)
	if existed {
		log.Debugf("Existing book found: %v", bl.name)
	} else {
		log.Debugf("New book added: %v", bl.name)
	}
	for _, a := range annotations {
		a.BookId = book.Id
		if e.annotationRepo.UpsertAnnotation(a) {
			e.annotationsUpdated++
		} else {
			e.annotationsInserted++
		}
	}
	return nil
}

func (e *ContentExtractor) processHighlightsForBook(body []byte) (book *model.Book, annotations []*model.Annotation, err error) {
	type annotationLink struct {
		url      string
		contents string
	}
	book = &model.Book{
		Name:    "",
		Authors: "",
		Isbn:    "",
	}
	var als []*annotationLink
	var al *annotationLink
	isBookAuthors := false
	isTitleAndISBN := false
	z := html.NewTokenizer(bytes.NewReader(body))
loop:
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			if errors.Is(z.Err(), io.EOF) {
				break loop
			}
			log.Warnf("Encountered unexpected error: %v", z.Err())
			return nil, nil, z.Err()
		case html.TextToken:
			if al != nil {
				annotationContents := string(z.Text())
				if len(strings.TrimSpace(annotationContents)) > 0 {
					al.contents = annotationContents
					als = append(als, al)
				}
			} else if isBookAuthors {
				authorsText := string(z.Text())
				if strings.HasPrefix(authorsText, "by ") {
					authorsText = authorsText[3:]
				}
				book.Authors = authorsText
			} else if isTitleAndISBN && book.Isbn != "" {
				titleText := string(z.Text())
				book.Name = titleText
			}
		case html.StartTagToken:
			name, hasAttr := z.TagName()
			at := atom.Lookup(name)
			if at == atom.A && hasAttr {
				isAnnotationLink := false
				href := ""
				for {
					k, v, more := z.TagAttr()
					vs := string(v)
					switch atom.Lookup(k) {
					case atom.Class:
						if strings.Contains(vs, "t-annotation-quote") {
							isAnnotationLink = true
						}
					case atom.Href:
						href = vs
					}
					if !more {
						break
					}
				}
				if isAnnotationLink {
					al = &annotationLink{
						url: href,
					}
				}
			} else if at == atom.Li && hasAttr {
				for {
					k, v, more := z.TagAttr()
					vs := string(v)
					switch atom.Lookup(k) {
					case atom.Class:
						if strings.Contains(vs, "t-annotation-authors") {
							isBookAuthors = true
						}
						if strings.Contains(vs, "media-title") {
							isTitleAndISBN = true
						}
					}
					if !more {
						break
					}
				}
			} else if at == atom.A && hasAttr && isTitleAndISBN {
				for {
					k, v, more := z.TagAttr()
					switch atom.Lookup(k) {
					case atom.Href:
						submatch := isbnRegex.FindAllSubmatch(v, -1)
						if len(submatch) == 0 {
							return nil, nil, fmt.Errorf("could not match the ISBN in the book page URL: %v", v)
						}
						book.Isbn = string(submatch[0][0])
					}
					if !more {
						break
					}
				}
			}
		case html.EndTagToken:
			al = nil
			isBookAuthors = false
			isTitleAndISBN = false
		}
	}

	for _, al := range als {
		var annotation *model.Annotation
		annotation = &model.Annotation{
			Text:     al.contents,
			Location: "",
			Ts:       time.Time{},
			Origin:   "oreilly",
			Type:     model.Highlight,
		}
		if strings.HasSuffix(al.contents, "...") {
			//TODO: visit annotation page to get the full quote
		}
		annotations = append(annotations, annotation)
	}

	return
}
