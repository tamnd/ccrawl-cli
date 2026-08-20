package ccrawl

import (
	"context"
	"sync"
	"time"
)

// hostClockCap is how many host request times are kept before the stale ones
// are swept out.
const hostClockCap = 1 << 16

// hostClock is the per host politeness clock, which is the only thing standing
// between a crawler and being a nuisance. It remembers when each host was last
// asked for something and holds a worker until that host's turn comes round.
//
// It lives on its own because two run loops need it and they have nothing else
// in common. The frontier crawl keeps a queue and follows links; the recrawl
// streams a list that is already written down. Both have to space their requests
// to a host, and a second copy of this logic is a second chance to get it subtly
// wrong.
type hostClock struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func newHostClock() *hostClock { return &hostClock{last: make(map[string]time.Time)} }

// wait holds until host may be asked for something, and stamps the clock at the
// moment the request is about to go out.
//
// The stamp is taken here rather than when the worker was handed the URL,
// because the two are not the same instant under load and the host only ever
// sees the first one. It happens under the lock, so two workers aiming at one
// host cannot both read a free slot and take it, and the one that loses
// recomputes its wait rather than sleeping through a slot it did not get.
func (c *hostClock) wait(ctx context.Context, host string, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	for {
		c.mu.Lock()
		now := time.Now()
		earliest := now
		if prev, ok := c.last[host]; ok {
			earliest = prev.Add(delay)
		}
		if !earliest.After(now) {
			c.last[host] = now
			if len(c.last) > hostClockCap {
				// A crawl of the open web sees millions of hosts and needs the
				// timings of none of them past the delay, so the map is swept
				// rather than grown.
				for h, t := range c.last {
					if t.Add(delay).Before(now) {
						delete(c.last, h)
					}
				}
			}
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()

		t := time.NewTimer(time.Until(earliest))
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// stamp records when a request to a host actually went out.
func (c *hostClock) stamp(host string, t time.Time) {
	c.mu.Lock()
	if prev, ok := c.last[host]; !ok || t.After(prev) {
		c.last[host] = t
	}
	c.mu.Unlock()
}
