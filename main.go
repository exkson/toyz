package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"exkson.tech/crytoviz/crawl"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const batchSize = 100

func main() {
	var flagUntil string
	flag.StringVar(&flagUntil, "until", "", "Fetch articles until this date (RFC3339 format: 2025-10-31T04:45:00Z)")
	flag.Parse()

	var until time.Time
	var err error

	// Try parsing as RFC3339 first, then fall back to YYYY-MM-DD format
	until, err = time.Parse(time.RFC3339, flagUntil)
	if err != nil {
		log.Fatalf("invalid date format: %v. Use RFC3339 (2025-10-31T04:45:00Z) or YYYY-MM-DD format", err)
	}

	log.Printf("going to fetch articles until %v", until.Format(time.RFC3339))

	if until.After(time.Now()) {
		log.Fatal("--until should be a date in the past")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if articles, err := crawl.ListCMCArticles(ctx, until); err == nil {
		mongoUri := os.Getenv("MONGO_URI")
		dbName := os.Getenv("MONGO_DB_NAME")
		if dbName == "" {
			dbName = "toyz"
		}
		client, err := mongo.Connect(options.Client().ApplyURI(mongoUri))

		if err != nil {
			log.Fatal(err)
		}

		defer func() {
			if err := client.Disconnect(context.TODO()); err != nil {
				panic(err)
			}
		}()

		collection := client.Database(dbName).Collection("articles")

		if len(articles) > 0 {
			for i := 0; i < len(articles); i += batchSize {
				end := min(i+batchSize, len(articles))
				batch := articles[i:end]
				var docs []map[string]any
				for _, doc := range batch {
					docs = append(docs, map[string]any{
						"url":        doc.Url,
						"date":       doc.CreatedAt,
						"title":      doc.Title,
						"content":    doc.Content,
						"assets":     doc.Assets,
						"crawled_at": time.Now(),
					})
				}

				if len(docs) > 0 {
					_, err := collection.InsertMany(ctx, docs)
					if err != nil {
						log.Printf("[warn] unable to save batch: %v", err)
					}
				}
			}
		}
	} else {
		log.Fatal(err)
	}
}
