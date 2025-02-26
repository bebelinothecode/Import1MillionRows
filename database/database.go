package database

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"syscall"
	"golang.org/x/term"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// ConnectToDatabase establishes a connection to the PostgreSQL database and returns the connection.
func ConnectToDatabase() (*sql.DB, error) {
	reader := bufio.NewReader(os.Stdin)

	// Read database name
	fmt.Print("Enter database name: ")
	dbname, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("error reading database name: %w", err)
	}
	dbname = strings.TrimSpace(dbname) // Remove newline character

	// Read host
	fmt.Print("Enter host: ")
	host, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("error reading host: %w", err)
	}
	host = strings.TrimSpace(host)

	// Read port
	fmt.Print("Enter port: ")
	port, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("error reading port: %w", err)
	}
	port = strings.TrimSpace(port)

	// Read username
	fmt.Print("Enter username: ")
	username, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("error reading username: %w", err)
	}
	username = strings.TrimSpace(username)

	// Read password
	fmt.Print("Enter password: ")
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return nil, fmt.Errorf("error reading password: %w", err)
	}
	defer func() {
		// Clear the password from memory
		for i := range bytePassword {
			bytePassword[i] = 0
		}
	}()
	password := string(bytePassword)

	// Construct the connection string
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", username, password, host, port, dbname)
	// fmt.Println("\nConnection string:", connStr)

	// Open the database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	// Verify the connection
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}

	fmt.Println("\nConnected to database successfully!")
	return db, nil
}