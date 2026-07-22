package ccrawl

// DomainRow is the projected schema for the Common Crawl domain-level web graph
// ranks published to open-index/ccrawl-domains. It is the six source fields with
// the reversed host key un-reversed into a plain registrable domain. No rows are
// dropped, added, or reordered; the source is already sorted by harmonic
// centrality so part-000 holds the top-ranked domains.
type DomainRow struct {
	Domain      string  `parquet:"domain"`
	HarmonicPos int64   `parquet:"harmonic_pos"`
	HarmonicVal float64 `parquet:"harmonic_val"`
	PagerankPos int64   `parquet:"pagerank_pos"`
	PagerankVal float64 `parquet:"pagerank_val"`
	NHosts      int64   `parquet:"n_hosts"`
}

// DomainColumns is the ordered list of output column names.
var DomainColumns = []string{
	"domain", "harmonic_pos", "harmonic_val", "pagerank_pos", "pagerank_val", "n_hosts",
}

// DomainRankURL is the single gzipped domain-ranks table for a web-graph release,
// the sibling of WebGraph.HostRankURL for the domain level.
func (g WebGraph) DomainRankURL() string {
	return g.BaseURL + "domain/" + g.ID + "-domain-ranks.txt.gz"
}
