package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"exkson.tech/crytoviz/crawl"
)

func main() {
	var flagUntil string
	flag.StringVar(&flagUntil, "until", "", "Fetch articles until this date (YYYY-MM-DD)")
	flag.Parse()
	log.Printf("going to fetch articles until %v", flagUntil)
	var until, err = time.Parse(time.RFC3339, flagUntil)
	if err != nil {
		log.Fatal(err)
	}

	if until.After(time.Now()) {
		log.Fatal("--until should be a date in the past")
	}

	// Use a very long timeout for scraping large history
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	// Define callback to print article as JSON immediately
	onArticle := func(article crawl.Article) {
		jsonArticle := map[string]any{
			"url":        article.Url,
			"title":      article.Title,
			"content":    article.Content,
			"assets":     article.Assets,
			"created_at": article.CreatedAt,
		}
		jsonBody, err := json.Marshal(jsonArticle)
		if err != nil {
			log.Printf("unable to dumps articles to json %v", err)
			return
		}
		fmt.Println(string(jsonBody))
	}

	if err := crawl.ListCMCArticles(ctx, until, onArticle); err != nil {
		log.Fatal(err)
	}
}
