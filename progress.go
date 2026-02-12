package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type progressCounter interface {
	BytesRead() int64
}

type atomicProgressCounter struct {
	read atomic.Int64
}

func (c *atomicProgressCounter) Add(n int64) {
	if n > 0 {
		c.read.Add(n)
	}
}

func (c *atomicProgressCounter) BytesRead() int64 {
	return c.read.Load()
}

type progressReporter struct {
	remotePath string
	s3Key      string
	totalBytes int64
	counter    progressCounter
	interval   time.Duration
	window     time.Duration
}

type progressSample struct {
	when  time.Time
	bytes int64
}

func (p *progressReporter) Start() func() {
	stop := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		samples := make([]progressSample, 0, int(p.window/p.interval)+2)
		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				bytes := p.counter.BytesRead()
				samples = append(samples, progressSample{when: now, bytes: bytes})
				samples = pruneOldSamples(samples, now, p.window)

				speed := rollingSpeed(samples)
				percent := 100.0
				if p.totalBytes > 0 {
					percent = (float64(bytes) / float64(p.totalBytes)) * 100
					if percent > 100 {
						percent = 100
					}
				}

				log.Printf("progress remote=%s key=%s transferred=%d/%d pct=%.1f%% speed_last_%ds=%.2f MiB/s",
					p.remotePath,
					p.s3Key,
					bytes,
					p.totalBytes,
					percent,
					int(p.window.Seconds()),
					speed/(1024*1024),
				)
			}
		}
	}()

	return func() {
		once.Do(func() {
			close(stop)
			wg.Wait()
		})
	}
}

func pruneOldSamples(samples []progressSample, now time.Time, window time.Duration) []progressSample {
	cutoff := now.Add(-window)
	idx := 0
	for idx < len(samples) && samples[idx].when.Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return samples
	}
	return samples[idx:]
}

func rollingSpeed(samples []progressSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	first := samples[0]
	last := samples[len(samples)-1]
	elapsed := last.when.Sub(first.when).Seconds()
	if elapsed <= 0 {
		return 0
	}
	delta := float64(last.bytes - first.bytes)
	if delta < 0 {
		return 0
	}
	return delta / elapsed
}
