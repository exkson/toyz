package main

import (
	"context"
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
	if articles, err := crawl.ListCMCArticles(ctx, until); err == nil {
		for _, article := range articles {
			fmt.Println(article.Url)
			fmt.Println(article.Title)
			fmt.Println(article.Assets)
			fmt.Println(article.Content)
			fmt.Println("-------------------------------------------------")
		}
	} else {
		log.Fatal(err)
	}
}
