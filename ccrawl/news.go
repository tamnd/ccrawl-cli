package ccrawl

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"regexp"
	"strings"
)

var reNewsPath = regexp.MustCompile(`CC-NEWS-(\d{4})(\d{2})\d{2}`)

// ListNewsFiles returns the CC-NEWS WARC files for a year and month. Pass
// month 0 to list every month of a year; pass year 0 to list everything found
// via the index page.
func ListNewsFiles(ctx context.Context, h *HTTPClient, year, month int) ([]NewsFile, error) {
	if year == 0 {
		return listAllNewsFiles(ctx, h)
	}
	var months []string
	if month == 0 {
		for m := 1; m <= 12; m++ {
			months = append(months, fmt.Sprintf("%04d/%02d", year, m))
		}
	} else {
		months = []string{fmt.Sprintf("%04d/%02d", year, month)}
	}
	var all []NewsFile
	for _, mon := range months {
		files, err := fetchNewsPaths(ctx, h, mon)
		if err != nil {
			continue
		}
		all = append(all, files...)
	}
	return all, nil
}

func listAllNewsFiles(ctx context.Context, h *HTTPClient) ([]NewsFile, error) {
	data, err := h.FetchBytes(ctx, DataBaseURL+"crawl-data/CC-NEWS/index.html")
	if err != nil {
		return nil, fmt.Errorf("fetch CC-NEWS index: %w", err)
	}
	re := regexp.MustCompile(`(\d{4}/\d{2})/`)
	seen := map[string]bool{}
	var all []NewsFile
	for _, m := range re.FindAllStringSubmatch(string(data), -1) {
		mon := m[1]
		if seen[mon] {
			continue
		}
		seen[mon] = true
		files, err := fetchNewsPaths(ctx, h, mon)
		if err != nil {
			continue
		}
		all = append(all, files...)
	}
	return all, nil
}

func fetchNewsPaths(ctx context.Context, h *HTTPClient, monthPath string) ([]NewsFile, error) {
	url := fmt.Sprintf("%scrawl-data/CC-NEWS/%s/warc.paths.gz", DataBaseURL, monthPath)
	resp, err := h.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	var files []NewsFile
	sc := bufio.NewScanner(gz)
	for sc.Scan() {
		p := strings.TrimSpace(sc.Text())
		if p == "" {
			continue
		}
		nf := NewsFile{Path: p}
		if m := reNewsPath.FindStringSubmatch(p); len(m) == 3 {
			nf.Year = atoi(m[1])
			nf.Mon = atoi(m[2])
		}
		files = append(files, nf)
	}
	return files, sc.Err()
}
