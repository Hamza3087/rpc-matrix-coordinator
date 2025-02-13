package main

import (
	"bufio"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Worker struct {
	mu            sync.RWMutex
	activeTasks   int
	maxTasks      int
	lastHeartbeat time.Time
	logger        *log.Logger
}

type ResourceStats struct {
	CPUUsage    float64
	MemoryUsage float64
	ActiveTasks int
}

func NewWorker() *Worker {
	logFile, err := os.OpenFile("worker.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	logger := log.New(logFile, "WORKER: ", log.LstdFlags|log.Lshortfile)

	return &Worker{
		maxTasks:      runtime.NumCPU() * 2, // Set max tasks based on CPU cores
		lastHeartbeat: time.Now(),
		logger:        logger,
	}
}

func (w *Worker) HealthCheck(_ struct{}, reply *bool) error {
	w.mu.Lock()
	w.lastHeartbeat = time.Now()
	w.mu.Unlock()
	*reply = true
	return nil
}

func (w *Worker) GetResourceStats(_ struct{}, reply *ResourceStats) error {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	w.mu.RLock()
	stats := ResourceStats{
		CPUUsage:    float64(w.activeTasks) / float64(w.maxTasks) * 100,
		MemoryUsage: float64(memStats.Alloc) / float64(memStats.Sys) * 100,
		ActiveTasks: w.activeTasks,
	}
	w.mu.RUnlock()

	*reply = stats
	return nil
}

// Add performs matrix addition with resource management
func (w *Worker) Add(matrices [2][][]int, reply *[][]int) error {


	fmt.Println("Sleeping for 2 seconds...")
	time.Sleep(20 * time.Second)
	fmt.Println("Woke up!")


	if err := w.checkResourceAvailability(); err != nil {
		return err
	}
	w.incrementActiveTasks()
	defer w.decrementActiveTasks()

	w.logger.Println("Performing addition")
	
	if len(matrices[0]) == 0 || len(matrices[1]) == 0 {
		return fmt.Errorf("empty matrix received")
	}

	rows, cols := len(matrices[0]), len(matrices[0][0])
	if len(matrices[1]) != rows || len(matrices[1][0]) != cols {
		return fmt.Errorf("matrix dimensions mismatch")
	}

	result := make([][]int, rows)
	for i := range result {
		result[i] = make([]int, cols)
		for j := 0; j < cols; j++ {
			result[i][j] = matrices[0][i][j] + matrices[1][i][j]
		}
	}

	*reply = result
	w.logger.Printf("Addition completed. Result size: %dx%d", len(result), len(result[0]))
	return nil
}

// Transpose performs matrix transposition with resource management
func (w *Worker) Transpose(matrix [][]int, reply *[][]int) error {
	if err := w.checkResourceAvailability(); err != nil {
		return err
	}
	w.incrementActiveTasks()
	defer w.decrementActiveTasks()

	w.logger.Println("Performing transpose")

	if len(matrix) == 0 {
		return fmt.Errorf("empty matrix received")
	}

	rows, cols := len(matrix), len(matrix[0])
	result := make([][]int, cols)
	for i := range result {
		result[i] = make([]int, rows)
		for j := 0; j < rows; j++ {
			result[i][j] = matrix[j][i]
		}
	}

	*reply = result
	w.logger.Printf("Transpose completed. Result size: %dx%d", len(result), len(result[0]))
	return nil
}

// Multiply performs matrix multiplication with resource management
func (w *Worker) Multiply(matrices [2][][]int, reply *[][]int) error {
	if err := w.checkResourceAvailability(); err != nil {
		return err
	}
	w.incrementActiveTasks()
	defer w.decrementActiveTasks()

	w.logger.Println("Performing multiplication")

	if len(matrices[0]) == 0 || len(matrices[1]) == 0 {
		return fmt.Errorf("empty matrix received")
	}

	if len(matrices[0][0]) != len(matrices[1]) {
		return fmt.Errorf("invalid dimensions for multiplication")
	}

	rows, cols := len(matrices[0]), len(matrices[1][0])
	result := make([][]int, rows)
	for i := range result {
		result[i] = make([]int, cols)
		for j := 0; j < cols; j++ {
			for k := 0; k < len(matrices[1]); k++ {
				result[i][j] += matrices[0][i][k] * matrices[1][k][j]
			}
		}
	}

	*reply = result
	w.logger.Printf("Multiplication completed. Result size: %dx%d", len(result), len(result[0]))
	return nil
}

func (w *Worker) checkResourceAvailability() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	if w.activeTasks >= w.maxTasks {
		return fmt.Errorf("worker at maximum capacity (%d/%d tasks)", w.activeTasks, w.maxTasks)
	}
	return nil
}

func (w *Worker) incrementActiveTasks() {
	w.mu.Lock()
	w.activeTasks++
	w.mu.Unlock()
}

func (w *Worker) decrementActiveTasks() {
	w.mu.Lock()
	w.activeTasks--
	w.mu.Unlock()
}

func (w *Worker) registerWithCoordinator(workerPort int, coordinatorAddr string) error {
	workerAddress := fmt.Sprintf("localhost:%d", workerPort)

	conn, err := tls.Dial("tcp", coordinatorAddr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return fmt.Errorf("error connecting to coordinator: %v", err)
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	var reply string
	err = client.Call("Coordinator.AddWorker", workerAddress, &reply)
	if err != nil {
		return fmt.Errorf("error registering with coordinator: %v", err)
	}

	w.logger.Printf("Successfully registered with coordinator at %s", coordinatorAddr)
	return nil
}

func main() {
	// Register types with gob
	gob.Register([][]int{})
	
	worker := NewWorker()
	worker.logger.Println("Starting worker setup")

	reader := bufio.NewReader(os.Stdin)

	// Get worker port
	fmt.Print("Enter port for this worker (e.g., 9001): ")
	portInput, _ := reader.ReadString('\n')
	portInput = strings.TrimSpace(portInput)

	workerPort, err := strconv.Atoi(portInput)
	if err != nil || workerPort <= 0 {
		worker.logger.Fatal("Invalid port number")
	}

	// Get coordinator address
	fmt.Print("Enter coordinator address (e.g., localhost:12345): ")
	coordinatorAddr, _ := reader.ReadString('\n')
	coordinatorAddr = strings.TrimSpace(coordinatorAddr)

	if !strings.Contains(coordinatorAddr, ":") {
		worker.logger.Fatal("Invalid coordinator address format")
	}

	// Register the worker for RPC
	rpc.Register(worker)

	// Load TLS certificates
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		worker.logger.Fatalf("Error loading certificates: %v", err)
	}

	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	listener, err := tls.Listen("tcp", fmt.Sprintf(":%d", workerPort), config)
	if err != nil {
		worker.logger.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()

	// Register with coordinator in a separate goroutine
	go func() {
		for {
			err := worker.registerWithCoordinator(workerPort, coordinatorAddr)
			if err != nil {
				worker.logger.Printf("Registration attempt failed: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			break
		}
	}()

	worker.logger.Printf("Worker server running on port %d", workerPort)

	// Start resource monitoring
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for range ticker.C {
			var stats ResourceStats
			worker.GetResourceStats(struct{}{}, &stats)
			worker.logger.Printf("Resource stats - CPU: %.2f%%, Memory: %.2f%%, Active Tasks: %d",
				stats.CPUUsage, stats.MemoryUsage, stats.ActiveTasks)
		}
	}()

	// Accept and serve RPC requests
	for {
		conn, err := listener.Accept()
		if err != nil {
			worker.logger.Printf("Error accepting connection: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
