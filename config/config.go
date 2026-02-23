package config

import (
	"fmt"
	"os"
	"strings"

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

type PayoutConfigs struct {
	Platforms map[string]PlatformConfig `json:"platforms"`
}

type PlatformConfig struct {
	ImportConfigs []ImportConfig `json:"import_configs,omitempty"`
	ExportConfigs []ExportConfig `json:"export_configs,omitempty"`
}

type ImportConfig struct {
	TableName     string        `json:"table_name,omitempty"`
	Sheet         string        `json:"sheet,omitempty"`
	Range         string        `json:"range,omitempty"`
	RelativeRange RelativeRange `json:"relative_range,omitempty"`
	Header        bool          `json:"header,omitempty"`
	StopAtEmpty   bool          `json:"stop_at_empty,omitempty"`
	AllVarchar    bool          `json:"all_varchar,omitempty"`
}

type ExportConfig struct {
	TableName     string             `json:"table_name,omitempty"`
	ReaderConfigs []DataReaderConfig `json:"reader_configs,omitempty"`
}

type RelativeRange struct {
	RelativeConfigIndex int `json:"relative_config_index,omitempty"`
	RowsOffset          int `json:"rows_offset,omitempty"`
}

type DataReaderConfig struct {
	ColumnName string `json:"column_name"`
	Expression string `json:"expression"`
}

func (p PlatformConfig) String() string {
	return fmt.Sprintf("PlatformConfig{ImportConfigs: %v, ExportConfigs: %v}", p.ImportConfigs, p.ExportConfigs)
}

func (p ImportConfig) ToOptionString() string {
	var options string

	if p.Header {
		options += "header=true,"
	}
	if p.StopAtEmpty {
		options += "stop_at_empty=true,"
	}
	if p.AllVarchar {
		options += "all_varchar=true,"
	}
	if p.Sheet != "" {
		options += fmt.Sprintf("sheet='%s',", p.Sheet)
	}
	if p.Range != "" {
		options += fmt.Sprintf("range='%s'", p.Range)
	}
	return options
}

func (p ExportConfig) ToSelectExpresssions() string {
	var expressions string
	for _, readerConfig := range p.ReaderConfigs {
		if expressions == "" {
			expressions = fmt.Sprintf("%s: %s", readerConfig.ColumnName, readerConfig.Expression)
		} else {
			expressions += fmt.Sprintf(", %s: %s", readerConfig.ColumnName, readerConfig.Expression)
		}
	}
	return fmt.Sprintf("{ %s }", expressions)
}

func (p ImportConfig) GetTableName(platform string) string {
	if p.TableName != "" {
		return p.TableName
	}
	return fmt.Sprintf("payout_%s_%s_%s", strings.ToLower(platform), strings.ReplaceAll(p.Sheet, " ", "_"), strings.ReplaceAll(p.Range, ":", "_"))
}

func (p ExportConfig) GetTableName(platform string) string {
	return p.TableName
}
