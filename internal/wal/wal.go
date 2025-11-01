package wal

import (
	"bufio"
	"os"
	"sync"
)

// Simple write ahead log.
// Stores every single action on disk so if the server dies we still know which actions to queue
type WAL struct {
	file *os.File
	mu   sync.Mutex
	path string
}

func NewWAL(path string) (*WAL, error) {
	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// put some entry at the end of the log
func (w *WAL) WriteEntry(entry string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.WriteString(entry + "\n")
	if err != nil {
		return err
	}

	// Sync to disk immediately for durability
	return w.file.Sync()
}

// MY TIME MY TIME REPLAY!!!
func ReadEntries(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil // No log file yet, so we dont say any error
		}
		// some sinister evil error that we should handle
		return nil, err
	}
	defer file.Close()

	var entries []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		entries = append(entries, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
