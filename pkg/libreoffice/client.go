package libreoffice

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// Client calls the LibreOffice-based spreadsheet parser service.
type Client struct {
	baseURL    string
	dataPath   string
	httpClient *http.Client
}

// ParseResult holds an ordered list of column headers and data rows returned by
// the LibreOffice parser service.
type ParseResult struct {
	Headers []string
	Rows    []map[string]interface{}
}

// parseResponse mirrors the JSON envelope returned by the service when
// as_table=true.  The service returns {"data": [...]} where each element is a
// row object whose keys are the (possibly multi-line) xlsx column headers.
// An optional "headers" key with an ordered list is accepted when present.
type parseResponse struct {
	Data    []map[string]interface{} `json:"data"`
	Headers []string                 `json:"headers"`
}

// sanitizeColumnName replaces newline and carriage-return characters with a
// single space and trims surrounding whitespace.  xlsx column headers that span
// multiple lines arrive with literal \n characters; sanitising them produces
// SQL-friendly identifiers that are easy to reference in export-config
// expressions.
func sanitizeColumnName(name string) string {
	name = strings.ReplaceAll(name, "\n", " ")
	name = strings.ReplaceAll(name, "\r", " ")
	return strings.TrimSpace(name)
}

// sanitizeRows returns a new slice of rows where every map key has been
// sanitised with sanitizeColumnName.
func sanitizeRows(rows []map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		sanitized := make(map[string]interface{}, len(row))
		for k, v := range row {
			sanitized[sanitizeColumnName(k)] = v
		}
		out[i] = sanitized
	}
	return out
}

// NewClient creates a LibreOffice parser client.
//
//   - baseURL  – base URL of the parser service (e.g. "http://localhost:8091")
//   - dataPath – root path of the Paperless media volume as visible to the
//     parser service (e.g. "/data")
func NewClient(baseURL, dataPath string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		dataPath:   strings.TrimRight(dataPath, "/"),
		httpClient: &http.Client{},
	}
}

// Parse fetches spreadsheet rows from the LibreOffice parser service.
//
// filePath is relative to the Paperless media root
// (e.g. "documents/originals/2022/01/invoice.xlsx").  The client prepends its
// configured dataPath so the service receives an absolute path.
func (c *Client) Parse(filePath, sheetName, rangeStr string, hasHeader, stopAtEmpty bool) (*ParseResult, error) {
	fullPath := c.dataPath + "/" + strings.TrimLeft(filePath, "/")

	params := url.Values{}
	params.Set("file_path", fullPath)
	if sheetName != "" {
		params.Set("sheet_name", sheetName)
	}
	if rangeStr != "" {
		params.Set("range", rangeStr)
	}
	params.Set("has_header", fmt.Sprintf("%t", hasHeader))
	params.Set("stop_at_empty", fmt.Sprintf("%t", stopAtEmpty))
	params.Set("as_table", "true")

	reqURL := fmt.Sprintf("%s/parse?%s", c.baseURL, params.Encode())
	slog.Debug("Calling LibreOffice parser", "url", reqURL)

	resp, err := c.httpClient.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("libreoffice parse request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("libreoffice returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read libreoffice response: %w", err)
	}

	// Try wrapped format: {"data": [...]} (actual LibreOffice service format).
	// An optional "headers" key with an ordered list may also be present.
	var wrapped parseResponse
	if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Data != nil {
		sanitizedHeaders := make([]string, len(wrapped.Headers))
		for i, h := range wrapped.Headers {
			sanitizedHeaders[i] = sanitizeColumnName(h)
		}
		return &ParseResult{Headers: sanitizedHeaders, Rows: sanitizeRows(wrapped.Data)}, nil
	}

	// Fallback: plain JSON array of row objects
	var rows []map[string]interface{}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("failed to parse libreoffice response: %w", err)
	}
	rows = sanitizeRows(rows)

	// Derive header list from the first row's (already sanitised) keys.  Go maps
	// have non-deterministic iteration order, so keys are sorted alphabetically
	// to keep the result deterministic.
	// NOTE: positional `#N` column references in export configs will NOT work
	// reliably; use named column references instead.
	var headers []string
	if len(rows) > 0 {
		for k := range rows[0] {
			headers = append(headers, k)
		}
		sort.Strings(headers)
	}

	return &ParseResult{Headers: headers, Rows: rows}, nil
}
