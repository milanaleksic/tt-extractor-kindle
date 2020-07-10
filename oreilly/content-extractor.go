package oreilly

import (
	"encoding/json"
	"fmt"
	"github.com/milanaleksic/tt-extractor-kindle/model"
	"github.com/milanaleksic/tt-extractor-kindle/utils"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"time"
)

const learningPlatformURL = "https://learning.oreilly.com"

type ContentExtractor struct {
	bookRepo            model.BookRepository
	annotationRepo      model.AnnotationRepository
	annotationsUpdated  int
	annotationsInserted int
	client              *http.Client
}

func NewContentExtractor(bookRepo model.BookRepository, annotationRepo model.AnnotationRepository, cookies map[string]string) *ContentExtractor {
	return &ContentExtractor{
		bookRepo:       bookRepo,
		annotationRepo: annotationRepo,
		client: &http.Client{
			Jar:     createCookieJar(cookies),
			Timeout: 10 * time.Second,
		},
	}
}

func createCookieJar(cookiesMap map[string]string) *cookiejar.Jar {
	jar, err := cookiejar.New(nil)
	utils.Check(err)
	parsedUrl, err := url.Parse(learningPlatformURL)
	utils.Check(err)
	var cookies []*http.Cookie
	for k, v := range cookiesMap {
		cookies = append(cookies, &http.Cookie{
			Name:  k,
			Value: v,
		})
	}
	jar.SetCookies(parsedUrl, cookies)
	return jar
}

func (e *ContentExtractor) IngestRecords() (err error) {
	highlightsPage, err := e.getHighlightsPageLocationFromProfile()
	if err != nil {
		return fmt.Errorf("could not get user id: %w", err)
	}

	response, err := e.client.Get(highlightsPage)
	if err != nil {
		return fmt.Errorf("failed to fetch %v: %w", highlightsPage, err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read body from %v: %w", highlightsPage, err)
	}
	log.Infof("Body of highlights page: %s", body)
	return nil
}

func (e *ContentExtractor) getHighlightsPageLocationFromProfile() (highlightsPage string, err error) {
	begin := time.Now()
	profilePage := fmt.Sprintf("%s/profile", learningPlatformURL)
	response, err := e.client.Get(profilePage)
	if err != nil {
		return "", fmt.Errorf("failed to fetch %v: %w", profilePage, err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read body from %v: %w", profilePage, err)
	}
	urlPattern := regexp.MustCompile(`window.initialStoreData ?= ?([^;]+);`)
	submatch := urlPattern.FindAllSubmatch(body, -1)
	if len(submatch) == 0 {
		log.Debugf("Unable to parse user ID anywhere on the profile page: %v", string(body))
		return "", fmt.Errorf("no match found for user id")
	}
	log.Infof("Read profile page in %dms", time.Now().Sub(begin).Milliseconds())
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

func (e *ContentExtractor) login() {

}
