package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Worker struct {
	conn       net.Conn
	reader     *bufio.Reader
	queueName  string
	shouldStop bool
}

// This is a thing that pretends to be an actual worker process that would process tasks from the task queue
func NewWorker(serverAddr, queueName string) (*Worker, error) {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return nil, err
	}

	return &Worker{
		conn:      conn,
		reader:    bufio.NewReader(conn),
		queueName: queueName,
	}, nil
}

func (w *Worker) sendCommand(cmd string) (string, error) {
	// Send command
	_, err := fmt.Fprintf(w.conn, "%s\n", cmd)
	if err != nil {
		return "", err
	}

	// Read response
	response, err := w.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(response), nil
}

func (w *Worker) processJobs() {
	fmt.Printf("Worker started, listening to queue: %s\n", w.queueName)

	for !w.shouldStop {
		// Try to dequeue a job
		response, err := w.sendCommand(fmt.Sprintf("DEQUEUE %s", w.queueName))
		if err != nil {
			log.Printf("Error dequeuing: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Parse response: "OK jobID data" or "ERROR message"
		parts := strings.SplitN(response, " ", 3)
		if len(parts) < 2 || parts[0] != "OK" {
			// Queue is empty or error occurred
			if strings.Contains(response, "no items left") {
				fmt.Println("Queue empty, waiting...")
			} else {
				fmt.Printf("Error response: %s\n", response)
			}
			time.Sleep(2 * time.Second)
			continue
		}

		if len(parts) < 3 {
			log.Printf("Invalid response format: %s", response)
			continue
		}

		jobID := parts[1]
		jobData := parts[2]

		fmt.Printf("Processing job %s: %s\n", jobID, jobData)

		// Simulate work (1-5 seconds)
		workTime := time.Duration(rand.Intn(5)+1) * time.Second
		fmt.Printf("Working for %v...\n", workTime)
		time.Sleep(workTime)

		// Acknowledge the job
		ackResp, err := w.sendCommand(fmt.Sprintf("ACK %s %s", w.queueName, jobID))
		if err != nil {
			log.Printf("Error acknowledging job %s: %v", jobID, err)
			continue
		}

		if strings.HasPrefix(ackResp, "OK") {
			fmt.Printf("Job %s completed and acknowledged\n", jobID)
		} else {
			log.Printf("Failed to ack job %s: %s", jobID, ackResp)
		}
	}
}

func (w *Worker) Close() {
	w.shouldStop = true
	w.conn.Close()
	fmt.Println("Worker shut down")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <queue_name>")
		fmt.Println("Example: go run main.go emails")
		os.Exit(1)
	}

	queueName := os.Args[1]

	worker, err := NewWorker("localhost:8080", queueName)
	if err != nil {
		log.Fatal("Failed to connect to server:", err)
	}
	defer worker.Close()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nReceived shutdown signal...")
		worker.Close()
		os.Exit(0)
	}()

	// Start processing
	worker.processJobs()
}
