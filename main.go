package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
	"github.com/example/database"
)

func main() {
	db, err := database.ConnectToDatabase()

	if err != nil {
		log.Fatal("Error connecting to database:",err)
	}

	defer db.Close()

	file, err := os.Open("data.csv")

	if err != nil {
		log.Fatal("Error opening file:", err)
	}

	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Error reading from file",err)
	}

	startTime := time.Now()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Error beginning a transaction:",err)
	}

	statement, err := tx.Prepare("INSERT INTO bebelino (name, email, phone, address) VALUES ($1, $2, $3, $4)")
	if err != nil {
		log.Fatal("Error preparing sql statement:",err)
	}
	defer statement.Close()

	var rowCount int
	for _, record := range records {
		_, err := statement.Exec(record[0], record[1], record[2], record[3])
		if err != nil {
			log.Fatal("Error inserting record",err)
			tx.Rollback()
			return
		}
		rowCount++
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal("Error commit transacction:", err)
	} 

	duration := time.Since(startTime)

	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	fmt.Println("✅ Data import completed successfully!")
	fmt.Printf("⏳ Execution Time: %s\n", duration)
	fmt.Printf("📊 Rows Affected: %d\n", rowCount)
	fmt.Printf("📌 Memory Used: %d KB\n", (memEnd.Alloc-memStart.Alloc)/1024)
}