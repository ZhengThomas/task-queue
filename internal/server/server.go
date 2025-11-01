package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ZhengThomas/task-queue/internal/config"
	"github.com/ZhengThomas/task-queue/internal/protocol"
	"github.com/ZhengThomas/task-queue/internal/queue"
	"github.com/ZhengThomas/task-queue/internal/wal"
)

type Server struct {
	queues   map[string]*queue.Queue
	mu       sync.RWMutex
	listener net.Listener
	wal      *wal.WAL
	metrics  *Metrics
	shutdown chan struct{}
}

// Creates a new server object without actually starting it to listen
func NewServer(cfg *config.ServerConfig) (*Server, error) {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, err
	}

	walPath := cfg.WALPath

	walLog, err := wal.NewWAL(walPath)
	if err != nil {
		listener.Close()
		return nil, err
	}

	srv := &Server{
		queues:   make(map[string]*queue.Queue),
		listener: listener,
		wal:      walLog,
		metrics:  NewMetrics(),
		shutdown: make(chan struct{}),
	}

	// Replay the log to restore state
	if err := srv.replayWAL(walPath); err != nil {
		listener.Close()
		walLog.Close()
		return nil, err
	}

	return srv, nil
}

func (s *Server) replayWAL(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := wal.ReadEntries(path)
	if err != nil {
		return err
	}

	fmt.Printf("Replaying %d entries from WAL...\n", len(entries))

	for _, entry := range entries {
		// We know that everything in the write ahead log (should be) valid
		parts := strings.SplitN(entry, " ", 2)

		if parts[0] == protocol.MsgEnqueue {
			toEnqueue := strings.Split(parts[1], " ")
			s.queues[toEnqueue[0]].Enqueue(strings.Join(toEnqueue[1:], " "))
		} else if parts[0] == protocol.MsgDequeue {
			s.queues[parts[1]].DequeueWithId()
		} else if parts[0] == protocol.MsgCreateQueue {
			s.queues[parts[1]] = queue.NewQueue()
		} else if parts[0] == protocol.MsgAck {
			ackParts := strings.Split(parts[1], " ")
			s.queues[ackParts[0]].Ack(ackParts[1])
		}

	}

	return nil
}

// Makes some server object listen for stuff
func (s *Server) Start(cfg *config.ServerConfig) error {
	fmt.Printf("Server listening on %s\n", s.listener.Addr().String())

	go s.timeoutChecker(cfg.GetTimeout(), cfg.GetTimeoutCheckInterval())

	// every once in a while we print out the metrics of the current run of the server
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Println(s.metrics.GetStats())
			case <-s.shutdown:
				return
			}
		}
	}()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		// Each connection is handle concurrently
		go s.handleConnection(conn)
	}
}

func (s *Server) timeoutChecker(timeoutTime time.Duration, timeoutCheckTime time.Duration) {
	// check for timeouts every once in a while
	ticker := time.NewTicker(timeoutCheckTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.RLock()
			for name, q := range s.queues {
				requeued := q.CheckTimeouts(timeoutTime)
				if len(requeued) > 0 {
					s.metrics.IncrementTimedOut(len(requeued))
					fmt.Printf("Requeued %d timed-out jobs from queue %s\n", len(requeued), name)
				}
			}
			s.mu.RUnlock()
		case <-s.shutdown:
			return // Exit when shutdown signal received
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		msg, err := protocol.ReadMessage(reader)
		if err != nil {
			return // Client disconnected, or something bad happened
		}

		if msg.Type == protocol.MsgEnqueue {
			// enqueue something
			parts := strings.Split(msg.Data, " ")
			if len(parts) < 2 {
				protocol.WriteMessage(writer, protocol.MsgError, "no data provided to enqueue")
			} else {
				err := s.Enqueue(parts[0], strings.Join(parts[1:], " "))
				if err != nil {
					protocol.WriteMessage(writer, protocol.MsgError, err.Error())
				} else {
					protocol.WriteMessage(writer, protocol.MsgOK, "enqueued data successfully")
				}
			}
		} else if msg.Type == protocol.MsgDequeue {
			id, val, err := s.Dequeue(msg.Data)
			if err != nil {
				protocol.WriteMessage(writer, protocol.MsgError, err.Error())
			} else {
				protocol.WriteMessage(writer, protocol.MsgOK, id+" "+val)
			}
		} else if msg.Type == protocol.MsgCreateQueue {
			parts := strings.Split(msg.Data, " ")
			if len(parts) > 1 {
				protocol.WriteMessage(writer, protocol.MsgError, "cant have spaces in name")
			} else if msg.Data == "" {
				protocol.WriteMessage(writer, protocol.MsgError, "queue name required")
			} else {
				err := s.CreateQueue(msg.Data)
				if err != nil {
					protocol.WriteMessage(writer, protocol.MsgError, err.Error())
				} else {
					protocol.WriteMessage(writer, protocol.MsgOK, "queue created successfully")
				}
			}
		} else if msg.Type == protocol.MsgAck {
			parts := strings.Split(msg.Data, " ")
			if len(parts) != 2 {
				protocol.WriteMessage(writer, protocol.MsgError, "Ack message only needs queue name and job id")
			} else {
				err := s.Ack(parts[0], parts[1])
				if err != nil {
					protocol.WriteMessage(writer, protocol.MsgError, err.Error())
				} else {
					protocol.WriteMessage(writer, protocol.MsgOK, fmt.Sprintf("job with id %s is acknowledged", parts[1]))
				}
			}
		} else if msg.Type == protocol.MsgListQueues {
			queues := s.ListQueues()
			protocol.WriteMessage(writer, protocol.MsgOK, strings.Join(queues, ","))
		} else {
			// We got some message we werent expecting at all
			protocol.WriteMessage(writer, protocol.MsgError, "Unexpected message type, check valid message types. Received type: "+msg.Type)
		}

		writer.Flush() // actually sends the response back
		fmt.Printf("Received: %s %s\n", msg.Type, msg.Data)
	}
}

func (s *Server) Enqueue(name string, value string) error {
	q, err := s.GetQueue(name)

	if err != nil {
		return err
	}

	// right before actually enqueueing, we add it to the write ahead log
	if err := s.wal.WriteEntry(fmt.Sprintf("ENQUEUE %s %s", name, value)); err != nil {
		return err
	}
	q.Enqueue(value)
	s.metrics.IncrementEnqueued()
	return nil
}

func (s *Server) Dequeue(name string) (string, string, error) {
	q, err := s.GetQueue(name)

	if err != nil {
		return "", "", err
	}

	// right before actually dequeueing. The only error that could occur later is that we
	// try to dequeue an empty queue, which is fine on replay
	if err := s.wal.WriteEntry(fmt.Sprintf("DEQUEUE %s", name)); err != nil {
		return "", "", err
	}
	id, item, ok := q.DequeueWithId()

	if !ok {
		return "", "", fmt.Errorf("no items left in queue named %v", name)
	}

	s.metrics.IncrementDequeued()
	return id, fmt.Sprintf("%v", item), nil
}

func (s *Server) CreateQueue(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[name]; exists {
		return fmt.Errorf("the queue %v already exists", name)
	}

	// right before actually creating a queue, we add it to the write ahead log
	if err := s.wal.WriteEntry(fmt.Sprintf("CREATE %s", name)); err != nil {
		return err
	}

	s.queues[name] = queue.NewQueue()
	return nil
}

func (s *Server) Ack(name string, jobId string) error {
	q, err := s.GetQueue(name)

	if err != nil {
		return err
	}

	// right before actually acknowledging. The only error that could occur later is that we
	// try to ack a job that doesnt exist, which wont break anything on replay
	if err := s.wal.WriteEntry(fmt.Sprintf("ACK %s %s", name, jobId)); err != nil {
		return err
	}

	// bring the error upwards (if it exists)
	err = q.Ack(jobId)
	if err != nil {
		return err
	}

	s.metrics.IncrementAcked()
	return nil
}

func (s *Server) GetQueue(name string) (*queue.Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	q, exists := s.queues[name]

	if !exists {
		return nil, fmt.Errorf("there is no queue named %v", name)
	}

	return q, nil
}

func (s *Server) ListQueues() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.queues))

	for name := range s.queues {
		names = append(names, name)
	}

	return names
}

func (s *Server) Close() error {
	close(s.shutdown) // Signal shutdown

	fmt.Println("\nShutting down gracefully...")
	fmt.Println("Final stats:", s.metrics.GetStats())

	// Close WAL
	if err := s.wal.Close(); err != nil {
		return err
	}

	// Close listener
	return s.listener.Close()
}
