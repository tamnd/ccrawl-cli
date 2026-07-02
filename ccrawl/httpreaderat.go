package ccrawl

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// httpReaderAt is an io.ReaderAt over a remote file, backed by HTTP range GETs and
// an aligned block cache. It is what lets parquet-go read a remote Parquet file with
// column projection without downloading the whole file: parquet-go reads the footer,
// then only the chunks of the columns the reader's schema selects, so a single-column
// projection over the Common Crawl index touches a fraction of the 530 MiB shard.
//
// Reads are served from fixed-size aligned blocks. A cache miss fetches one block with
// a range request; a small LRU keeps the working set bounded because parquet reads a
// column chunk forward, so old blocks are never revisited. The bytes actually fetched
// are counted so a caller can report the real network cost against the file size.
type httpReaderAt struct {
	ctx   context.Context
	h     *HTTPClient
	url   string
	size  int64
	block int64
	cap   int // max cached blocks

	mu     sync.Mutex
	cache  map[int64][]byte // block index -> bytes
	order  []int64          // LRU order, oldest first
	fetched atomic.Int64
}

// newHTTPReaderAt builds a reader over url of the given size. blockSize and cacheBlocks
// bound the cache; zero values pick sensible defaults (8 MiB blocks, 8 cached).
func newHTTPReaderAt(ctx context.Context, h *HTTPClient, url string, size, blockSize int64, cacheBlocks int) *httpReaderAt {
	if blockSize <= 0 {
		blockSize = 8 << 20
	}
	if cacheBlocks <= 0 {
		cacheBlocks = 8
	}
	return &httpReaderAt{
		ctx:   ctx,
		h:     h,
		url:   url,
		size:  size,
		block: blockSize,
		cap:   cacheBlocks,
		cache: make(map[int64][]byte, cacheBlocks),
	}
}

// BytesFetched is the total number of bytes pulled over the network so far.
func (r *httpReaderAt) BytesFetched() int64 { return r.fetched.Load() }

// ReadAt satisfies io.ReaderAt. It copies out of the block cache, fetching any block the
// requested span touches that is not resident.
func (r *httpReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	end := min(off+int64(len(p)), r.size)
	n := 0
	for pos := off; pos < end; {
		bi := pos / r.block
		blk, err := r.blockAt(bi)
		if err != nil {
			return n, err
		}
		start := pos - bi*r.block
		copied := copy(p[n:], blk[start:])
		n += copied
		pos += int64(copied)
	}
	if off+int64(n) >= r.size && n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// blockAt returns block bi from the cache, fetching it on a miss.
func (r *httpReaderAt) blockAt(bi int64) ([]byte, error) {
	r.mu.Lock()
	if b, ok := r.cache[bi]; ok {
		r.touch(bi)
		r.mu.Unlock()
		return b, nil
	}
	r.mu.Unlock()

	// Fetch outside the lock so concurrent readers on distinct blocks do not serialize
	// on the network. A duplicate fetch of the same block under a race is harmless.
	blk, err := r.fetch(bi)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	if b, ok := r.cache[bi]; ok {
		r.touch(bi)
		r.mu.Unlock()
		return b, nil
	}
	r.cache[bi] = blk
	r.order = append(r.order, bi)
	r.evict()
	r.mu.Unlock()
	return blk, nil
}

// fetch pulls block bi with a range request.
func (r *httpReaderAt) fetch(bi int64) ([]byte, error) {
	start := bi * r.block
	length := r.block
	if start+length > r.size {
		length = r.size - start
	}
	resp, err := r.h.GetRange(r.ctx, r.url, start, length)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		return nil, fmt.Errorf("range GET %s: status %d", r.url, resp.StatusCode)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return nil, fmt.Errorf("read range body: %w", err)
	}
	r.fetched.Add(length)
	return buf, nil
}

// touch moves bi to the newest position in the LRU order. Caller holds the lock.
func (r *httpReaderAt) touch(bi int64) {
	for i, v := range r.order {
		if v == bi {
			r.order = append(r.order[:i], r.order[i+1:]...)
			r.order = append(r.order, bi)
			return
		}
	}
}

// evict drops the oldest blocks until the cache is within cap. Caller holds the lock.
func (r *httpReaderAt) evict() {
	for len(r.order) > r.cap {
		oldest := r.order[0]
		r.order = r.order[1:]
		delete(r.cache, oldest)
	}
}
