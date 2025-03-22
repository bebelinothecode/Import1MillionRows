package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
	"github.com/example/database"
	"github.com/lib/pq"
)

// Configuration constants
const (
	workerCount = 5
	batchSize   = 100
	tableName   = "bebelino" 
	csvFile     = "data.csv"
)

func main() {
	// Connect to database
	db, err := database.ConnectToDatabase()
	if err != nil {
		log.Fatal("Error connecting to database:", err)
	}
	defer db.Close()

	// Open the CSV file
	file, err := os.Open(csvFile)
	if err != nil {
		log.Fatal("Error opening file:", err)
	}
	defer file.Close()

	// Start performance measurement
	startTime := time.Now()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read header row
	header, err := reader.Read()
	if err != nil {
		log.Fatal("Error reading header:", err)
	}

	// Setup channels for worker communication
	dataChan := make(chan []string, workerCount*2) // Buffer to prevent blocking
	resultChan := make(chan struct {
		count int
		err   error
	}, workerCount)

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go worker(db, tableName, header, dataChan, resultChan, &wg, i)
	}

	// Read and send records to workers without loading everything into memory
	go func() {
		recordCount := 0
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading record: %v", err)
				continue
			}
			dataChan <- record
			recordCount++
		}
		close(dataChan)
		log.Printf("Total records sent to processing: %d", recordCount)
	}()

	// Wait for workers to finish in a separate goroutine
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results
	successCount := 0
	errorCount := 0
	
	for result := range resultChan {
		if result.err != nil {
			log.Printf("⚠️ Worker error: %v", result.err)
			errorCount++
		} else {
			successCount += result.count
		}
	}

	// Calculate and display performance metrics
	duration := time.Since(startTime)
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)
	memUsed := (memEnd.Alloc - memStart.Alloc) / 1024

	fmt.Println("✅ Data import completed!")
	fmt.Printf("⏳ Execution Time: %s\n", duration)
	fmt.Printf("✓ Records Processed: %d\n", successCount)
	fmt.Printf("⚠️ Errors Encountered: %d\n", errorCount)
	fmt.Printf("📌 Memory Used: %d KB\n", memUsed)
	
	if errorCount > 0 {
		fmt.Println("⚠️ Some records failed to import. Check the logs for details.")
	}
}

// Worker function for parallel processing
func worker(
	db *sql.DB,
	tableName string,
	header []string,
	dataChan <-chan []string,
	resultChan chan<- struct {count int; err error},
	wg *sync.WaitGroup,
	workerID int,
) {
	defer wg.Done()
	
	recordsProcessed := 0
	batch := make([][]string, 0, batchSize)

	// Process records in batches
	for record := range dataChan {
		batch = append(batch, record)
		
		// When we reach batch size, process the batch
		if len(batch) >= batchSize {
			if err := processBatch(db, tableName, header, batch); err != nil {
				resultChan <- struct {count int; err error}{0, fmt.Errorf("worker %d batch error: %v", workerID, err)}
			} else {
				recordsProcessed += len(batch)
			}
			batch = make([][]string, 0, batchSize) // Reset batch
		}
	}
	
	// Process any remaining records
	if len(batch) > 0 {
		if err := processBatch(db, tableName, header, batch); err != nil {
			resultChan <- struct {count int; err error}{0, fmt.Errorf("worker %d final batch error: %v", workerID, err)}
		} else {
			recordsProcessed += len(batch)
		}
	}
	
	resultChan <- struct {count int; err error}{recordsProcessed, nil}
	log.Printf("Worker %d completed, processed %d records", workerID, recordsProcessed)
}

// Process a batch of records
func processBatch(db *sql.DB, tableName string, header []string, batch [][]string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("transaction start error: %v", err)
	}
	
	// Use defer with a named return variable to handle rollback correctly
	var txErr error
	defer func() {
		if txErr != nil {
			tx.Rollback()
		}
	}()

	// Prepare COPY statement
	stmt, err := tx.Prepare(pq.CopyIn(tableName, header...))
	if err != nil {
		txErr = err
		return fmt.Errorf("prepare statement error: %v", err)
	}
	defer stmt.Close()

	// Execute for all records in batch
	for _, record := range batch {
		args := make([]any, len(record))
		for i, v := range record {
			args[i] = v
		}

		_, err := stmt.Exec(args...)
		if err != nil {
			txErr = err
			return fmt.Errorf("insert error: %v", err)
		}
	}

	// Execute the buffered copy
	_, err = stmt.Exec()
	if err != nil {
		txErr = err
		return fmt.Errorf("statement execution error: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		txErr = err
		return fmt.Errorf("commit error: %v", err)
	}
	return nil
}