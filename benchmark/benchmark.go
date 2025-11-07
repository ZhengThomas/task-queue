package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type BenchmarkStats struct {
	enqueuedCount int64
	dequeuedCount int64
	ackedCount    int64
	errorCount    int64
}

func (s *BenchmarkStats) String(duration time.Duration) string {
	enqueued := atomic.LoadInt64(&s.enqueuedCount)
	dequeued := atomic.LoadInt64(&s.dequeuedCount)
	acked := atomic.LoadInt64(&s.ackedCount)
	errors := atomic.LoadInt64(&s.errorCount)

	seconds := duration.Seconds()
	enqRate := float64(enqueued) / seconds
	deqRate := float64(dequeued) / seconds
	ackRate := float64(acked) / seconds

	return fmt.Sprintf(`
Benchmark Results (%v)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total Enqueued:    %d (%.0f ops/sec)
Total Dequeued:    %d (%.0f ops/sec)
Total Acked:       %d (%.0f ops/sec)
Errors:            %d
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`, duration.Round(time.Millisecond),
		enqueued, enqRate,
		dequeued, deqRate,
		acked, ackRate,
		errors)
}

func createQueue(serverAddr, queueName string) error {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "CREATE %s\n", queueName)
	reader := bufio.NewReader(conn)
	reader.ReadString('\n')
	return nil
}

func dequeueWorker(serverAddr, queueName string, stats *BenchmarkStats, wg *sync.WaitGroup, stop *atomic.Bool) {
	defer wg.Done()

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		atomic.AddInt64(&stats.errorCount, 1)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for !stop.Load() {
		// Dequeue
		fmt.Fprintf(conn, "DEQUEUE %s\n", queueName)
		response, err := reader.ReadString('\n')
		if err != nil {
			atomic.AddInt64(&stats.errorCount, 1)
			break
		}

		// Check if we got a job
		if len(response) < 3 || response[:2] != "OK" {
			time.Sleep(10 * time.Millisecond) // Queue empty, brief pause
			continue
		}

		atomic.AddInt64(&stats.dequeuedCount, 1)

		// Parse job ID from response: "OK jobID data"
		parts := response[3:] // Skip "OK "
		spaceIdx := 0
		for i, c := range parts {
			if c == ' ' {
				spaceIdx = i
				break
			}
		}

		if spaceIdx == 0 {
			continue
		}

		jobID := parts[:spaceIdx]

		// ACK the job
		fmt.Fprintf(conn, "ACK %s %s\n", queueName, jobID)
		_, err = reader.ReadString('\n')
		if err != nil {
			atomic.AddInt64(&stats.errorCount, 1)
			break
		}

		atomic.AddInt64(&stats.ackedCount, 1)
	}
}

func enqueueWorker(serverAddr, queueName string, stats *BenchmarkStats, wg *sync.WaitGroup, stop *atomic.Bool) {
	defer wg.Done()

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		atomic.AddInt64(&stats.errorCount, 1)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	jobNum := 0

	// Keep sending until told to stop
	for !stop.Load() {
		fmt.Fprintf(conn, "ENQUEUE %s job_data_%d\n", queueName, jobNum)
		_, err := reader.ReadString('\n')
		if err != nil {
			atomic.AddInt64(&stats.errorCount, 1)
			break
		}
		atomic.AddInt64(&stats.enqueuedCount, 1)
		jobNum++
	}
}

func runBenchmark(serverAddr string, numProducers, numConsumers int, duration time.Duration) {
	queueName := fmt.Sprintf("bench_%d", time.Now().Unix())
	stats := &BenchmarkStats{}

	fmt.Printf("Starting benchmark:\n")
	fmt.Printf("  Producers: %d\n", numProducers)
	fmt.Printf("  Consumers: %d\n", numConsumers)
	fmt.Printf("  Duration: %v\n\n", duration)

	if err := createQueue(serverAddr, queueName); err != nil {
		fmt.Printf("Failed to create queue: %v\n", err)
		return
	}

	startTime := time.Now()
	var wg sync.WaitGroup
	var stop atomic.Bool

	// Start consumers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go dequeueWorker(serverAddr, queueName, stats, &wg, &stop)
	}

	time.Sleep(100 * time.Millisecond)

	// Start producers (they now run until stopped)
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go enqueueWorker(serverAddr, queueName, stats, &wg, &stop)
	}

	// Let it run for the duration
	time.Sleep(duration)

	// Stop everyone
	stop.Store(true)
	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Println(stats.String(elapsed))
}

func main() {
	serverAddr := "localhost:8080"

	fmt.Println("=== Task Queue Benchmark Suite ===\n")

	fmt.Println("Test 1: Single producer/consumer")
	runBenchmark(serverAddr, 1, 1, 10*time.Second)
	time.Sleep(2 * time.Second)

	fmt.Println("\nTest 2: Multiple producers (10), single consumer")
	runBenchmark(serverAddr, 10, 1, 10*time.Second)
	time.Sleep(2 * time.Second)

	fmt.Println("\nTest 3: Single producer, multiple consumers (10)")
	runBenchmark(serverAddr, 1, 10, 10*time.Second)
	time.Sleep(2 * time.Second)

	fmt.Println("\nTest 4: High concurrency (20 producers, 20 consumers)")
	runBenchmark(serverAddr, 20, 20, 10*time.Second)

	fmt.Println("\n=== Benchmark Complete ===")
}
