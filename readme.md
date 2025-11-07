# Distributed Task Queue

A lightweight, persistent distributed task queue system similar to Celery or BullMQ, built in Go with support for multiple named queues, job acknowledgements, and automatic timeout handling.

## Features
- Multiple named queues
- Persistent storage with Write-Ahead Logging (WAL)
- Job acknowledgement with automatic retry on timeout
- TCP-based wire protocol
- Graceful shutdown
- Built-in metrics

## Quick Start
You can run the following for a quick overview of what the task queue can do

**Terminal 1: Start server**  
go run cmd/server/main.go

**Terminal 2: Add some jobs**  
go run examples/producer/main.go emails "Send welcome email"  
go run examples/producer/main.go emails "Send password reset"  
go run examples/producer/main.go emails "Send notification"  

**Terminal 3: Start worker**   
go run examples/worker/main.go emails

## Basic use case
Run the following:  
"go run cmd/server/main.go"  
to start the task queue server and send the server any of the commands in protocol.go. These are the following:  

**Enqueue a new item**
"ENQUEUE queuename some potentially space seperated value"  

**Dequeue a task from the task queue.** This will give back data in the form "id potentially space seperated task name"  
the id will be important in the ack command    
"DEQUEUE queuename"  

**Create a new queue**  
"CREATE queuename"  

**Acknowledge that as task has been completed**. If a task has not been completed in some set amount of time, it will be requeued  
"ACK jobid"  

**List queue names**  
"LIST"  

You can run  
"go run cmd/client/main.go"  
If you would like a client app that directly sends the task queue commands for testing purposes  

## Architecture
The main block of code is found within internal/server/server.go. This is where it sends and receives messages. You can find the general logic within internal/queue/queue.go, which governs how and what is inserted and removed from the task queue. The rest of the files are mainly helper files that do other miscellaneous tasks

## Configuration
the config.yaml file is a very simple file that determines certain pieces of whats ran on the server. alter it if there are certain features that you would like changed.
