package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <queue_name> <message>")
		fmt.Println("Example: go run main.go emails 'Send welcome email to user@example.com'")
		os.Exit(1)
	}

	queueName := os.Args[1]
	message := strings.Join(os.Args[2:], " ")

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Create queue
	fmt.Fprintf(conn, "CREATE %s\n", queueName)
	response, _ := reader.ReadString('\n')
	fmt.Print("Create queue: ", response)

	// Enqueue message
	fmt.Fprintf(conn, "ENQUEUE %s %s\n", queueName, message)
	response, _ = reader.ReadString('\n')
	fmt.Print("Enqueue: ", response)

	fmt.Println("Job added successfully!")
}
