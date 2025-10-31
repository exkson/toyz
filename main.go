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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// empty slice
	var jsonArticles []map[string]any
	if articles, err := crawl.ListCMCArticles(ctx, until); err == nil {
		for _, article := range articles {
			jsonArticles = append(jsonArticles, map[string]any{
				"url":        article.Url,
				"title":      article.Title,
				"content":    article.Content,
				"assets":     article.Assets,
				"created_at": article.CreatedAt,
			})
			jsonBody, err := json.Marshal(jsonArticles)
			if err != nil {
				log.Fatalf("unable to dumps articles to json %v", err)
			}
			fmt.Println(string(jsonBody))
		}
	} else {
		log.Fatal(err)
	}
}
