package crawl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func ListCMCArticles(ctx context.Context, until time.Time) ([]Article, error) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		},
	}
	page := 1
	oldest := time.Now()

	articlesChannel := make(chan Article)
	var wg sync.WaitGroup

	for oldest.After(until) {
		articles, err := fetchArticles(ctx, client, page)
		if err != nil {
			log.Printf("[WARN] got %v on page %d", err, page)
			continue
		}
		log.Printf("sucessfully fetched page %d", page)
		page++
		oldest = articles[len(articles)-1].CreatedAt
		for _, articleOverview := range articles {
			wg.Add(1)
			go fetchArticle(articleOverview.Url, client, articlesChannel, &wg)
		}
	}

	go func() {
		wg.Wait()
		close(articlesChannel)
	}()

	allArticles := make([]Article, 0)
	for article := range articlesChannel {
		allArticles = append(allArticles, article)
	}
	return allArticles, nil
}

func fetchArticles(ctx context.Context, client *http.Client, page int) ([]ArticleOverview, error) {
	articles := make([]ArticleOverview, 0, 20)
	body, err := json.Marshal(map[string]any{
		"mode":          "LATEST",
		"pageCreatedAt": page,
		"size":          20,
		"language":      "en",
		"newsTypes": []string{
			"NEWS",
			"ALEXANDRIA",
		},
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.coinmarketcap.com/aggr/v4/content/user", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("got invalid status code : %d", resp.StatusCode)
	}

	var result struct {
		Data []struct {
			Meta struct {
				Title     string `json:"title"`
				SourceUrl string `json:"sourceUrl"`
			} `json:"meta"`
			CreatedAt string `json:"createdAt"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	for _, item := range result.Data {
		timestamp, _ := time.Parse(time.RFC3339, item.CreatedAt)
		articles = append(articles, ArticleOverview{
			Url:       item.Meta.SourceUrl,
			CreatedAt: timestamp,
			Title:     item.Meta.Title,
		})
	}
	return articles, nil
}

func fetchArticle(url string, client *http.Client, channel chan Article, wg *sync.WaitGroup) {
	defer wg.Done()
	r, err := client.Get(url)
	if err != nil {
		return
	}
	if r.StatusCode != http.StatusOK {
		return
	}
	defer r.Body.Close()

	doc, err := goquery.NewDocumentFromReader(r.Body)
	if err != nil {
		log.Printf("[WARN] got error parsing %v : %v", url, err)
	}

	title := doc.Find("h1").Text()
	assets := []string{}
	doc.Find(".eqkKtR span").Each(func(i int, s *goquery.Selection) {
		assets = append(assets, s.Text())
	})
	content := doc.Find("article").Text()
	if content == "" && strings.Contains(url, "community") {
		var data struct {
			Props struct {
				PageProps struct {
					Article struct {
						Content string `json:"content"`
					} `json:"article"`
				} `json:"pageProps"`
			} `json:"props"`
		}
		json.NewDecoder(bytes.NewBuffer([]byte(doc.Find("script#__NEXT_DATA__").Text()))).Decode(&data)

		articleDoc, err := goquery.NewDocumentFromReader(bytes.NewBuffer([]byte(data.Props.PageProps.Article.Content)))
		if err != nil {
			log.Printf("unable to parse retrieved article from next data")
			return
		}
		content = articleDoc.Text()
	}

	log.Printf("%v crawled", title)
	channel <- Article{
		Url:       url,
		Title:     title,
		Content:   content,
		Assets:    assets,
		CreatedAt: time.Now(),
	}
}
