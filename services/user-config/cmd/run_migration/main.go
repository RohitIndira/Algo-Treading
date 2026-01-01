package main

import (
    "bufio"
    "database/sql"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"

    _ "github.com/lib/pq"
)

func loadEnv(path string) (map[string]string, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    env := make(map[string]string)
    r := bufio.NewReader(file)
    for {
        line, err := r.ReadString('\n')
        if err != nil && err != io.EOF {
            return nil, err
        }
        line = strings.TrimSpace(line)
        if len(line) == 0 || strings.HasPrefix(line, "#") {
            if err == io.EOF {
                break
            }
            if err != nil {
                break
            }
            continue
        }
        parts := strings.SplitN(line, "=", 2)
        if len(parts) == 2 {
            key := strings.TrimSpace(parts[0])
            val := strings.TrimSpace(parts[1])
            env[key] = strings.Trim(val, "\"')")
        }
        if err == io.EOF {
            break
        }
    }
    return env, nil
}

func main() {
    // locate repo root based on executable CWD
    cwd, _ := os.Getwd()
    // assume running from repository root or inside services/user-config
    envPath := filepath.Join(cwd, "services", "user-config", ".env")
    sqlPath := filepath.Join(cwd, "services", "user-config", "migrations", "002_add_depth_fields.sql")

    env, err := loadEnv(envPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to load env from %s: %v\n", envPath, err)
        os.Exit(1)
    }

    host := env["DB_HOST"]
    port := env["DB_PORT"]
    user := env["DB_USER"]
    password := env["DB_PASSWORD"]
    dbname := env["DB_NAME"]
    sslmode := env["DB_SSLMODE"]
    if sslmode == "" {
        sslmode = "disable"
    }

    dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, user, password, dbname, sslmode)

    sqlBytes, err := os.ReadFile(sqlPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to read sql file %s: %v\n", sqlPath, err)
        os.Exit(1)
    }

    db, err := sql.Open("postgres", dsn)
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to open db: %v\n", err)
        os.Exit(1)
    }
    defer db.Close()

    if err := db.Ping(); err != nil {
        fmt.Fprintf(os.Stderr, "failed to ping db: %v\n", err)
        os.Exit(1)
    }

    // execute SQL
    _, err = db.Exec(string(sqlBytes))
    if err != nil {
        fmt.Fprintf(os.Stderr, "migration execution failed: %v\n", err)
        os.Exit(1)
    }

    fmt.Println("Migration applied successfully")
}
