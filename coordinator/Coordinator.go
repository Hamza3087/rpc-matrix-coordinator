package main

import (
	"container/heap"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkerStatus struct {
	Address     string
	ActiveTasks int
	LastActive  time.Time
	Health      bool
	Client      *rpc.Client
}

type Task struct {
	Timestamp time.Time
	Data      [3]interface{}
	Result    *[][]int
	Done      chan error
}

// PriorityQueue implementation for FCFS
type TaskQueue []*Task

func (pq TaskQueue) Len() int { return len(pq) }
func (pq TaskQueue) Less(i, j int) bool {
	return pq[i].Timestamp.Before(pq[j].Timestamp)
}
func (pq TaskQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *TaskQueue) Push(x interface{}) { *pq = append(*pq, x.(*Task)) }
func (pq *TaskQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

type Coordinator struct {
	workers       map[string]*WorkerStatus
	taskQueue    TaskQueue
	mu           sync.RWMutex
	logger       *log.Logger
	maxWorkerLoad int
}

func NewCoordinator() *Coordinator {
	logFile, err := os.OpenFile("coordinator.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	logger := log.New(logFile, "COORDINATOR: ", log.LstdFlags|log.Lshortfile)

	c := &Coordinator{
		workers:       make(map[string]*WorkerStatus),
		taskQueue:    make(TaskQueue, 0),
		logger:       logger,
		maxWorkerLoad: 5, // Maximum tasks per worker
	}
	heap.Init(&c.taskQueue)
	
	go c.monitorWorkers()
	go c.processTasks()
	
	return c
}

func (c *Coordinator) monitorWorkers() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		c.mu.Lock()
		for addr, worker := range c.workers {
			if err := worker.Client.Call("Worker.HealthCheck", struct{}{}, &worker.Health); err != nil {
				c.logger.Printf("Worker %s health check failed: %v", addr, err)
				worker.Health = false
				// Try to reconnect
				if client, err := c.connectToWorker(addr); err == nil {
					worker.Client = client
					worker.Health = true
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) connectToWorker(address string) (*rpc.Client, error) {
	conn, err := tls.Dial("tcp", address, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func (c *Coordinator) AssignTask(task [3]interface{}, reply *[][]int) error {
	c.logger.Printf("Received new task: %v", task[0])
	
	taskObj := &Task{
		Timestamp: time.Now(),
		Data:      task,
		Result:    reply,
		Done:      make(chan error, 1),
	}

	c.mu.Lock()
	heap.Push(&c.taskQueue, taskObj)
	c.mu.Unlock()

	// Wait for task completion
	err := <-taskObj.Done
	return err
}

func (c *Coordinator) AddWorker(address string, reply *string) error {
	c.logger.Printf("Adding worker: %s", address)

	client, err := c.connectToWorker(address)
	if err != nil {
		return fmt.Errorf("failed to connect to worker: %v", err)
	}

	c.mu.Lock()
	c.workers[address] = &WorkerStatus{
		Address:     address,
		ActiveTasks: 0,
		LastActive:  time.Now(),
		Health:      true,
		Client:      client,
	}
	c.mu.Unlock()

	*reply = "Worker added successfully"
	return nil
}

func (c *Coordinator) getLeastBusyWorker() *WorkerStatus {
	var selected *WorkerStatus
	minTasks := c.maxWorkerLoad + 1

	for _, worker := range c.workers {
		if worker.Health && worker.ActiveTasks < minTasks {
			selected = worker
			minTasks = worker.ActiveTasks
		}
	}
	return selected
}

func (c *Coordinator) processTasks() {
	for {
		c.mu.Lock()
		if c.taskQueue.Len() == 0 {
			c.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		task := heap.Pop(&c.taskQueue).(*Task)
		c.mu.Unlock()

		// Find least busy worker
		worker := c.getLeastBusyWorker()
		if worker == nil {
			task.Done <- fmt.Errorf("no available workers")
			continue
		}

		// Execute task
		worker.ActiveTasks++
		go func(w *WorkerStatus, t *Task) {
			err := w.Client.Call(t.Data[0].(string), t.Data[1], t.Result)
			
			c.mu.Lock()
			w.ActiveTasks--
			w.LastActive = time.Now()
			c.mu.Unlock()
			
			t.Done <- err
		}(worker, task)
	}
}

func main() {
	gob.Register([][]int{})
	gob.Register([2][][]int{})

	coordinator := NewCoordinator()
	rpc.Register(coordinator)

	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		coordinator.logger.Fatalf("Error loading certificates: %v", err)
	}

	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	listener, err := tls.Listen("tcp", ":12345", config)
	if err != nil {
		coordinator.logger.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()

	coordinator.logger.Println("Server running on port 12345")
	rpc.Accept(listener)
}