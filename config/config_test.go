package config

import (
	"os"
	"testing"
)

func boolPtr(b bool) *bool { return &b }

// TestLoad_MissingRequiredEnvVars verifies that Load returns an error for each
// required environment variable that is absent.
func TestLoad_MissingRequiredEnvVars(t *testing.T) {
	requiredEnvs := map[string]string{
		"PAPERLESS_URL":               "PAPERLESS_URL is required",
		"PAPERLESS_TOKEN":             "PAPERLESS_TOKEN is required",
		"GOOGLE_CLOUD_PROJECT":        "GOOGLE_CLOUD_PROJECT is required",
		"GOOGLE_CLOUD_LOCATION":       "GOOGLE_CLOUD_LOCATION is required",
		"DOCUMENT_AI_PROCESSOR_ID":    "DOCUMENT_AI_PROCESSOR_ID is required",
		"BANK_STATEMENT_PROCESSOR_ID": "BANK_STATEMENT_PROCESSOR_ID is required",
	}

	allRequired := map[string]string{
		"PAPERLESS_URL":               "http://paperless:8000",
		"PAPERLESS_TOKEN":             "tok",
		"GOOGLE_CLOUD_PROJECT":        "proj",
		"GOOGLE_CLOUD_LOCATION":       "us",
		"DOCUMENT_AI_PROCESSOR_ID":    "proc",
		"BANK_STATEMENT_PROCESSOR_ID": "bsproc",
	}

	for omit, wantMsg := range requiredEnvs {
		t.Run("missing_"+omit, func(t *testing.T) {
			// Set all required vars, then clear the one being tested.
			for k, v := range allRequired {
				t.Setenv(k, v)
			}
			t.Setenv(omit, "")

			_, err := Load()
			if err == nil {
				t.Fatalf("expected error when %s is missing, got nil", omit)
			}
			if err.Error() != wantMsg {
				t.Errorf("want error %q, got %q", wantMsg, err.Error())
			}
		})
	}
}

func TestLoad_AllRequiredEnvVars(t *testing.T) {
	t.Setenv("PAPERLESS_URL", "http://paperless:8000")
	t.Setenv("PAPERLESS_TOKEN", "mytoken")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "myproject")
	t.Setenv("GOOGLE_CLOUD_LOCATION", "us")
	t.Setenv("DOCUMENT_AI_PROCESSOR_ID", "myprocessor")
	t.Setenv("BANK_STATEMENT_PROCESSOR_ID", "mybanksprocessor")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PaperlessURL != "http://paperless:8000" {
		t.Errorf("unexpected PaperlessURL: %s", cfg.PaperlessURL)
	}
	if cfg.PaperlessToken != "mytoken" {
		t.Errorf("unexpected PaperlessToken: %s", cfg.PaperlessToken)
	}
}

func TestLoad_DefaultValues(t *testing.T) {
	t.Setenv("PAPERLESS_URL", "http://paperless:8000")
	t.Setenv("PAPERLESS_TOKEN", "tok")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "proj")
	t.Setenv("GOOGLE_CLOUD_LOCATION", "us")
	t.Setenv("DOCUMENT_AI_PROCESSOR_ID", "proc")
	t.Setenv("BANK_STATEMENT_PROCESSOR_ID", "bsproc")

	// Unset optional variables so that getEnv falls back to its default values.
	for _, key := range []string{"PORT", "DB_PATH", "TIKA_URL", "LOG_LEVEL", "LIBREOFFICE_DATA_PATH"} {
		orig, exists := os.LookupEnv(key)
		if exists {
			os.Unsetenv(key)
			t.Cleanup(func() { os.Setenv(key, orig) })
		}
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != "80" {
		t.Errorf("expected default Port '80', got %s", cfg.Port)
	}
	if cfg.DBPath != "data/duck.db" {
		t.Errorf("expected default DBPath 'data/duck.db', got %s", cfg.DBPath)
	}
	if cfg.TikaURL != "http://localhost:9998" {
		t.Errorf("expected default TikaURL 'http://localhost:9998', got %s", cfg.TikaURL)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected default LogLevel 'info', got %s", cfg.LogLevel)
	}
	if cfg.LibreOfficeDataPath != "/data" {
		t.Errorf("expected default LibreOfficeDataPath '/data', got %s", cfg.LibreOfficeDataPath)
	}
}

// TestGetEnv_UsesEnvVar verifies the fallback helper through exported Load behaviour.
func TestGetEnv_UsesEnvVar(t *testing.T) {
	t.Setenv("PAPERLESS_URL", "http://paperless:8000")
	t.Setenv("PAPERLESS_TOKEN", "tok")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "proj")
	t.Setenv("GOOGLE_CLOUD_LOCATION", "us")
	t.Setenv("DOCUMENT_AI_PROCESSOR_ID", "proc")
	t.Setenv("BANK_STATEMENT_PROCESSOR_ID", "bsproc")
	t.Setenv("PORT", "9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != "9090" {
		t.Errorf("expected Port '9090' from env, got %s", cfg.Port)
	}
}

// TestImportConfig_ToOptionString verifies the SQL option string builder.
func TestImportConfig_ToOptionString(t *testing.T) {
	trueVal := true
	falseVal := false

	tests := []struct {
		name   string
		cfg    ImportConfig
		expect string
	}{
		{
			name:   "empty config",
			cfg:    ImportConfig{},
			expect: "",
		},
		{
			name:   "header true",
			cfg:    ImportConfig{Header: &trueVal},
			expect: "header=true,",
		},
		{
			name:   "header false",
			cfg:    ImportConfig{Header: &falseVal},
			expect: "header=false,",
		},
		{
			name:   "stop_at_empty true",
			cfg:    ImportConfig{StopAtEmpty: &trueVal},
			expect: "stop_at_empty=true,",
		},
		{
			name:   "all_varchar true",
			cfg:    ImportConfig{AllVarchar: &trueVal},
			expect: "all_varchar=true,",
		},
		{
			name:   "sheet and range",
			cfg:    ImportConfig{Sheet: "Sheet1", Range: "A1:Z100"},
			expect: "sheet='Sheet1',range='A1:Z100'",
		},
		{
			name:   "all options",
			cfg:    ImportConfig{Header: &trueVal, StopAtEmpty: &falseVal, AllVarchar: &falseVal, Sheet: "Data", Range: "B2:D10"},
			expect: "header=true,stop_at_empty=false,all_varchar=false,sheet='Data',range='B2:D10'",
		},
		{
			name:   "nil bool pointers not emitted",
			cfg:    ImportConfig{Sheet: "Sheet2"},
			expect: "sheet='Sheet2',",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.ToOptionString()
			if got != tt.expect {
				t.Errorf("ToOptionString() = %q, want %q", got, tt.expect)
			}
		})
	}
}

// TestImportConfig_GetTableName verifies dynamic table name generation.
func TestImportConfig_GetTableName(t *testing.T) {
	tests := []struct {
		name     string
		cfg      ImportConfig
		platform string
		want     string
	}{
		{
			name:     "explicit table name",
			cfg:      ImportConfig{TableName: "my_table"},
			platform: "swiggy",
			want:     "my_table",
		},
		{
			name:     "generated from platform/sheet/range",
			cfg:      ImportConfig{Sheet: "Order Level", Range: "A1:Z100"},
			platform: "swiggy",
			want:     "payout_swiggy_Order_Level_A1_Z100",
		},
		{
			name:     "platform uppercased is lowercased",
			cfg:      ImportConfig{Sheet: "Data", Range: "B:D"},
			platform: "Zomato",
			want:     "payout_zomato_Data_B_D",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.GetTableName(tt.platform)
			if got != tt.want {
				t.Errorf("GetTableName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPlatformConfig_UseLibreOffice verifies method detection.
func TestPlatformConfig_UseLibreOffice(t *testing.T) {
	tests := []struct {
		name   string
		cfg    PlatformConfig
		expect bool
	}{
		{"duckdb method", PlatformConfig{Method: "duckdb"}, false},
		{"libreoffice lowercase", PlatformConfig{Method: "libreoffice"}, true},
		{"libreoffice mixed case", PlatformConfig{Method: "LibreOffice"}, true},
		{"LIBREOFFICE uppercase", PlatformConfig{Method: "LIBREOFFICE"}, true},
		{"empty method", PlatformConfig{Method: ""}, false},
		{"other method", PlatformConfig{Method: "tika"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.UseLibreOffice()
			if got != tt.expect {
				t.Errorf("UseLibreOffice() = %v, want %v", got, tt.expect)
			}
		})
	}
}

// TestExportConfig_ToSelectExpresssions verifies the DuckDB SELECT expression builder.
func TestExportConfig_ToSelectExpresssions(t *testing.T) {
	tests := []struct {
		name   string
		cfg    ExportConfig
		expect string
	}{
		{
			name:   "empty readers",
			cfg:    ExportConfig{},
			expect: "{  }",
		},
		{
			name: "single reader",
			cfg: ExportConfig{
				ReaderConfigs: []DataReaderConfig{
					{ColumnName: "outlet_name", Expression: "column('Restaurant Name')"},
				},
			},
			expect: "{ outlet_name: column('Restaurant Name') }",
		},
		{
			name: "multiple readers",
			cfg: ExportConfig{
				ReaderConfigs: []DataReaderConfig{
					{ColumnName: "outlet_name", Expression: "col1"},
					{ColumnName: "total_orders", Expression: "col2"},
				},
			},
			expect: "{ outlet_name: col1, total_orders: col2 }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.ToSelectExpresssions()
			if got != tt.expect {
				t.Errorf("ToSelectExpresssions() = %q, want %q", got, tt.expect)
			}
		})
	}
}

// TestExportConfig_GetTableName verifies that the table name is passed through.
func TestExportConfig_GetTableName(t *testing.T) {
	cfg := ExportConfig{TableName: "payout_swiggy"}
	if got := cfg.GetTableName("swiggy"); got != "payout_swiggy" {
		t.Errorf("GetTableName() = %q, want %q", got, "payout_swiggy")
	}
}

// TestPlatformConfig_String exercises the String() method for coverage.
func TestPlatformConfig_String(t *testing.T) {
	cfg := PlatformConfig{
		ImportConfigs: []ImportConfig{{TableName: "tbl"}},
		ExportConfigs: []ExportConfig{{TableName: "exp"}},
	}
	s := cfg.String()
	if s == "" {
		t.Error("PlatformConfig.String() returned empty string")
	}
}
