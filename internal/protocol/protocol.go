package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Message types
const (
	MsgEnqueue     = "ENQUEUE"
	MsgDequeue     = "DEQUEUE"
	MsgCreateQueue = "CREATE"
	MsgAck         = "ACK"
	MsgListQueues  = "LIST"
	MsgOK          = "OK"
	MsgError       = "ERROR"
)

// Really simple text based protocol
// Request: COMMAND [args]\n
// Response: OK [data]\n or ERROR [message]\n

// We should only be able to send TYPE, DATA messages over network with this
type Message struct {
	Type string
	Data string
}

// ReadMessage reads a message from a connection
func ReadMessage(reader *bufio.Reader) (*Message, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	parts := strings.SplitN(line, " ", 2)

	msg := &Message{
		Type: parts[0],
	}

	if len(parts) > 1 {
		msg.Data = parts[1]
	}

	return msg, nil
}

// WriteMessage writes a message to a connection
func WriteMessage(writer io.Writer, msgType string, data string) error {
	var line string
	if data != "" {
		line = fmt.Sprintf("%s %s\n", msgType, data)
	} else {
		line = fmt.Sprintf("%s\n", msgType)
	}

	_, err := writer.Write([]byte(line))
	return err
}
