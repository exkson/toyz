package crawl

import "time"

type ArticleOverview struct {
	Title     string
	Url       string
	CreatedAt time.Time
}
type Article struct {
	Url       string
	Title     string
	Content   string
	Assets    []string
	CreatedAt time.Time
}
