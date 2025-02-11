package main

import (
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Config struct {
	CoordinatorAddress string
	MaxRetries         int
	TimeoutSeconds     int
	MaxMatrixSize      int
}

type Request struct {
	Rows      int     `json:"rows"`
	Cols      int     `json:"cols"`
	Operation string  `json:"operation"`
	Matrix1   [][]int `json:"matrix1"`
	Matrix2   [][]int `json:"matrix2"`
}

type Response struct {
	Result [][]int `json:"result"`
	Error  string  `json:"error,omitempty"`
}

var (
	config = Config{
		CoordinatorAddress: "localhost:12345",
		MaxRetries:         3,
		TimeoutSeconds:     30,
		MaxMatrixSize:      1000,
	}
	logger *log.Logger
)

func init() {
	gob.Register([][]int{})
	gob.Register([2][][]int{})
	
	// Initialize logger
	logFile, err := os.OpenFile("client.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	logger = log.New(logFile, "CLIENT: ", log.LstdFlags|log.Lshortfile)
	logger.Println("Client initialized")
}

func validateMatrices(req Request) error {
	if req.Rows <= 0 || req.Cols <= 0 {
		return fmt.Errorf("invalid matrix dimensions")
	}
	if req.Rows > config.MaxMatrixSize || req.Cols > config.MaxMatrixSize {
		return fmt.Errorf("matrix dimensions exceed maximum allowed size")
	}

	validateDimensions := func(matrix [][]int, expectedRows, expectedCols int) error {
		if len(matrix) != expectedRows {
			return fmt.Errorf("invalid matrix rows")
		}
		for _, row := range matrix {
			if len(row) != expectedCols {
				return fmt.Errorf("invalid matrix columns")
			}
		}
		return nil
	}

	if err := validateDimensions(req.Matrix1, req.Rows, req.Cols); err != nil {
		return err
	}

	switch req.Operation {
	case "add":
		if err := validateDimensions(req.Matrix2, req.Rows, req.Cols); err != nil {
			return fmt.Errorf("matrices must have same dimensions for addition")
		}
	case "multiply":
		if len(req.Matrix2) == 0 || len(req.Matrix1[0]) != len(req.Matrix2) {
			return fmt.Errorf("invalid matrices for multiplication")
		}
	case "transpose":
		// No additional validation needed for transpose
	default:
		return fmt.Errorf("unsupported operation: %s", req.Operation)
	}

	return nil
}

func connectToCoordinator() (*rpc.Client, error) {
	var client *rpc.Client
	var err error

	for retry := 0; retry < config.MaxRetries; retry++ {
		conn, err := tls.Dial("tcp", config.CoordinatorAddress, &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			client = rpc.NewClient(conn)
			return client, nil
		}
		logger.Printf("Retry %d: Failed to connect to coordinator: %v", retry+1, err)
		time.Sleep(time.Second * time.Duration(retry+1))
	}

	return nil, fmt.Errorf("failed to connect after %d retries: %v", config.MaxRetries, err)
}

func handleOperation(w http.ResponseWriter, r *http.Request) {
	logger.Printf("Received %s request from %s", r.Method, r.RemoteAddr)

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Printf("Error decoding request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := validateMatrices(req); err != nil {
		logger.Printf("Validation error: %v", err)
		json.NewEncoder(w).Encode(Response{Error: err.Error()})
		return
	}

	client, err := connectToCoordinator()
	if err != nil {
		logger.Printf("Connection error: %v", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer client.Close()

	var result [][]int
	task := [3]interface{}{}

	switch req.Operation {
	case "add":
		task = [3]interface{}{"Worker.Add", [2][][]int{req.Matrix1, req.Matrix2}, nil}
	case "multiply":
		task = [3]interface{}{"Worker.Multiply", [2][][]int{req.Matrix1, req.Matrix2}, nil}
	case "transpose":
		task = [3]interface{}{"Worker.Transpose", req.Matrix1, nil}
	}

	logger.Printf("Sending task to coordinator: %v", task)
	
	// Set timeout for the RPC call
	done := make(chan error, 1)
	go func() {
		done <- client.Call("Coordinator.AssignTask", task, &result)
	}()

	select {
	case err := <-done:
		if err != nil {
			logger.Printf("Task error: %v", err)
			json.NewEncoder(w).Encode(Response{Error: err.Error()})
			return
		}
	case <-time.After(time.Duration(config.TimeoutSeconds) * time.Second):
		logger.Printf("Task timeout after %d seconds", config.TimeoutSeconds)
		json.NewEncoder(w).Encode(Response{Error: "operation timed out"})
		return
	}

	logger.Printf("Task completed successfully")
	json.NewEncoder(w).Encode(Response{Result: result})
}

func main() {
	logger.Println("Starting client server")

	http.Handle("/", http.FileServer(http.Dir("./")))
	http.HandleFunc("/operation", handleOperation)

	port := ":8081"
	logger.Printf("Server running on http://localhost%s", port)

	if err := http.ListenAndServe(port, nil); err != nil {
		logger.Fatalf("Server error: %v", err)
	}
}