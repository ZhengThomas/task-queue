package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ZhengThomas/task-queue/internal/config"
	"github.com/ZhengThomas/task-queue/internal/server"
)

func main() {
	cfg, err := config.LoadConfig("config.yaml")
	srv, err := server.NewServer(&cfg.Server)
	if err != nil {
		log.Fatal(err)
	}

	// handle shutdown command
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nReceived shutdown signal...")
		srv.Close()
		os.Exit(0)
	}()

	fmt.Println("Starting task queue server...")
	if err := srv.Start(&cfg.Server); err != nil {
		log.Fatal(err)
	}
}

/*
// You could and probably should ignore this block of code
func main() {
	q := queue.NewQueue()
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Task Queue Simple CLI")
	fmt.Println("Commands: enqueue <item>, dequeue, exit")

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		parts := strings.Fields(input) // Split by whitespace

		if len(parts) == 0 {
			continue
		}

		command := parts[0]

		switch command {
		case "enqueue":
			if len(parts) < 2 {
				fmt.Println("You need to give me an item to enqueue")
			} else {
				q.Enqueue(strings.Join(parts[1:], " "))
			}

		case "dequeue":
			item, ok := q.Dequeue()
			if ok {
				fmt.Println("Queue returned", item)
			} else {
				fmt.Println("Queue is empty or some unknown error occurred")
			}

		case "exit":
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}
*/
