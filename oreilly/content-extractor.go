package oreilly

import (
	"fmt"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	"github.com/milanaleksic/tt-extractor-kindle/utils"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"time"
)

type ContentExtractor struct {
	bookRepo            model.BookRepository
	annotationRepo      model.AnnotationRepository
	annotationsUpdated  int
	annotationsInserted int
	cookies             map[string]string
}

func NewContentExtractor(bookRepo model.BookRepository, annotationRepo model.AnnotationRepository, cookies map[string]string) *ContentExtractor {
	return &ContentExtractor{
		bookRepo:       bookRepo,
		annotationRepo: annotationRepo,
		cookies:        cookies,
	}
}

func (e *ContentExtractor) IngestRecords() {
	begin := time.Now()

	jar, err := cookiejar.New(nil)
	utils.Check(err)
	parsedUrl, err := url.Parse("https://learning.oreilly.com")
	utils.Check(err)
	var cookies []*http.Cookie

	for k, v := range e.cookies {
		cookies = append(cookies, &http.Cookie{
			Name:  k,
			Value: v,
		})
	}

	jar.SetCookies(parsedUrl, cookies)
	client := http.Client{
		Jar: jar,
	}
	response, err := client.Get("https://learning.oreilly.com/profile/")
	utils.Check(err)
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	utils.Check(err)
	fmt.Println("", string(body))

	log.Infof("Ingestion completed from oreilly %v in %dms; updated %v annotations and created %v new ones",
		time.Now().Sub(begin).Milliseconds(), e.annotationsUpdated, e.annotationsInserted)
}

func (e *ContentExtractor) login() {

}
