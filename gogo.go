package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type StressTestResult struct {
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	TotalDuration   time.Duration
	AvgLatency      time.Duration
	MinLatency      time.Duration
	MaxLatency      time.Duration
}

func main() {
	url := flag.String("url", "https://127.0.0.1", "Target URL (default: https://127.0.0.1)")
	workers := flag.Int("workers", 1000, "Number of concurrent workers (default: 1000)")
	duration := flag.Int("duration", 60, "Test duration in seconds (default: 60)")
	insecure := flag.Bool("insecure", true, "Skip TLS certificate verification for self-signed certs")
	connections := flag.Int("connections", 100, "Max idle connections per worker (default: 100)")

	flag.Parse()

	fmt.Printf("🔥 Starting stress test\n")
	fmt.Printf("Target: %s\n", *url)
	fmt.Printf("Workers: %d\n", *workers)
	fmt.Printf("Duration: %d seconds\n", *duration)
	fmt.Printf("Insecure TLS: %v\n", *insecure)
	fmt.Printf("Max idle connections: %d\n\n", *connections)

	// Create HTTP client with optimized transport
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		MaxIdleConns:        *connections,
		MaxIdleConnsPerHost: *connections,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		DisableCompression:  true,
	}

	if *insecure {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	// Metrics
	var totalRequests int64
	var successRequests int64
	var failedRequests int64
	var totalLatency int64
	var minLatency int64 = 1e18
	var maxLatency int64

	// Sync primitives
	var mu sync.Mutex
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					start := time.Now()
					resp, err := client.Get(*url)
					latency := time.Since(start)

					atomic.AddInt64(&totalRequests, 1)
					if err == nil {
						atomic.AddInt64(&successRequests, 1)
						io.ReadAll(resp.Body)
						resp.Body.Close()
					} else {
						atomic.AddInt64(&failedRequests, 1)
					}

					// Update latency stats (with mutex for min/max)
					latencyNs := latency.Nanoseconds()
					atomic.AddInt64(&totalLatency, latencyNs)

					mu.Lock()
					if latencyNs < minLatency {
						minLatency = latencyNs
					}
					if latencyNs > maxLatency {
						maxLatency = latencyNs
					}
					mu.Unlock()
				}
			}
		}()
	}

	// Statistics reporter
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	lastRequests := int64(0)
	testStart := time.Now()

	go func() {
		for range ticker.C {
			current := atomic.LoadInt64(&totalRequests)
			rps := current - lastRequests
			lastRequests = current
			elapsed := time.Since(testStart).Seconds()
			fmt.Printf("[%5.1fs] Requests: %d | RPS: %d | Success: %d | Failed: %d\n",
				elapsed,
				current,
				rps,
				atomic.LoadInt64(&successRequests),
				atomic.LoadInt64(&failedRequests))
		}
	}()

	// Wait for test duration
	time.Sleep(time.Duration(*duration) * time.Second)
	close(stopCh)

	// Wait for all workers to finish
	wg.Wait()

	// Final results
	testDuration := time.Since(testStart)
	totalReqs := atomic.LoadInt64(&totalRequests)
	successReqs := atomic.LoadInt64(&successRequests)
	failedReqs := atomic.LoadInt64(&failedRequests)

	var avgLatency time.Duration
	if totalReqs > 0 {
		avgLatency = time.Duration(atomic.LoadInt64(&totalLatency) / totalReqs)
	}

	fmt.Printf("\n✅ Stress test completed!\n\n")
	fmt.Printf("📊 Results:\n")
	fmt.Printf("  Total Requests:    %d\n", totalReqs)
	fmt.Printf("  Successful:        %d (%.1f%%)\n", successReqs, float64(successReqs)/float64(totalReqs)*100)
	fmt.Printf("  Failed:            %d (%.1f%%)\n", failedReqs, float64(failedReqs)/float64(totalReqs)*100)
	fmt.Printf("  Total Duration:    %v\n", testDuration)
	fmt.Printf("  Requests/sec:      %.0f\n", float64(totalReqs)/testDuration.Seconds())
	fmt.Printf("  Avg Latency:       %v\n", avgLatency)
	fmt.Printf("  Min Latency:       %v\n", time.Duration(minLatency))
	fmt.Printf("  Max Latency:       %v\n", time.Duration(maxLatency))
}
