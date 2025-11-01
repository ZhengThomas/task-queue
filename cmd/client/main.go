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
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Connected to server. Type commands (ENQUEUE <data>, DEQUEUE, exit):")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "exit" {
			return
		}

		// Send command to server
		fmt.Fprintf(conn, "%s\n", input)

		// Read response
		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal("Connection lost:", err)
		}

		fmt.Print("Server: ", response)
	}
}
