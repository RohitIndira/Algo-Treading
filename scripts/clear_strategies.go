package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	// Database connection
	dbHost := "localhost"
	dbPort := "5432"
	dbUser := "postgres"
	dbPassword := "postgres"
	dbName := "trading_db"

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	fmt.Println("Connected to PostgreSQL")
	fmt.Println("")

	// Clear all strategies in order of foreign key dependencies
	tables := []string{
		"trade_signals",
		"trade_configs",
		"risk_limits",
		"strategy_conditions",
		"strategies",
	}

	for _, table := range tables {
		query := fmt.Sprintf("DELETE FROM %s;", table)
		result, err := db.Exec(query)
		if err != nil {
			fmt.Printf("⚠ Error clearing %s: %v\n", table, err)
			continue
		}
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("✓ Cleared %s (%d rows deleted)\n", table, rowsAffected)
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM strategies").Scan(&count)
	if err != nil {
		log.Fatalf("Error querying strategies count: %v", err)
	}

	fmt.Println("")
	fmt.Printf("Final check: %d strategies remaining\n", count)
	fmt.Println("")
	fmt.Println("Cleanup Complete!")
}
