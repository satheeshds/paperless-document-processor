package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                  string
	DBPath                string
	PaperlessURL          string
	PaperlessToken        string
	GoogleProjectID       string
	GoogleLocation        string
	DocumentAIProcessorID string
	GoogleCredentialsPath string // Optional
	LogLevel              string
	PayoutConfigPath      string // JSON file for platform options

	// Accounting (optional)
	AccountingURL  string
	AccountingUser string
	AccountingPass string

	// Tika (optional, used for payout XLSX)
	TikaURL string
}

func Load() (*Config, error) {
	// Attempt to load .env file, but don't fail if it doesn't exist (e.g., prod env)
	_ = godotenv.Load()

	cfg := &Config{
		Port:                  getEnv("PORT", "80"),
		DBPath:                getEnv("DB_PATH", "data/duck.db"),
		PaperlessURL:          os.Getenv("PAPERLESS_URL"),
		PaperlessToken:        os.Getenv("PAPERLESS_TOKEN"),
		GoogleProjectID:       os.Getenv("GOOGLE_CLOUD_PROJECT"),
		GoogleLocation:        os.Getenv("GOOGLE_CLOUD_LOCATION"),
		DocumentAIProcessorID: os.Getenv("DOCUMENT_AI_PROCESSOR_ID"),
		GoogleCredentialsPath: os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),

		AccountingURL:  os.Getenv("ACCOUNTING_URL"),
		AccountingUser: os.Getenv("ACCOUNTING_USER"),
		AccountingPass: os.Getenv("ACCOUNTING_PASS"),

		TikaURL:          getEnv("TIKA_URL", "http://localhost:9998"),
		PayoutConfigPath: os.Getenv("PAYOUT_EXCEL_DUCKDB_CONFIG_PATH"),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.PaperlessURL == "" {
		return fmt.Errorf("PAPERLESS_URL is required")
	}
	if c.PaperlessToken == "" {
		return fmt.Errorf("PAPERLESS_TOKEN is required")
	}
	if c.GoogleProjectID == "" {
		return fmt.Errorf("GOOGLE_CLOUD_PROJECT is required")
	}
	if c.GoogleLocation == "" {
		return fmt.Errorf("GOOGLE_CLOUD_LOCATION is required")
	}
	if c.DocumentAIProcessorID == "" {
		return fmt.Errorf("DOCUMENT_AI_PROCESSOR_ID is required")
	}
	return nil
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
