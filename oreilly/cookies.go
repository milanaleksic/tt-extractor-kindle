package oreilly

import (
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
)

func createCookieJar(cookiesMap map[string]string) (*cookiejar.Jar, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("creation of empty cookie jar failed: %w", err)
	}
	parsedUrl, err := url.Parse(learningPlatformURL)
	if err != nil {
		return nil, fmt.Errorf("parsing of the URL to the learning platform failed: %w", err)
	}
	var cookies []*http.Cookie
	for k, v := range cookiesMap {
		if strings.Contains(v, `"`) {
			continue
		}
		cookies = append(cookies, &http.Cookie{
			Name:  k,
			Value: v,
		})
	}
	jar.SetCookies(parsedUrl, cookies)
	return jar, nil
}
