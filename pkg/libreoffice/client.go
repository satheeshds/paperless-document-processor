package libreoffice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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

// filterRefRows removes rows that contain any "#REF!" string value -- Excel
// formula-error cells that appear in Zomato payout files (e.g. the first
// summary row that references deleted cells).
func filterRefRows(rows []map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		hasRef := false
		for _, v := range row {
			if s, ok := v.(string); ok && s == "#REF!" {
				hasRef = true
				break
			}
		}
		if !hasRef {
			out = append(out, row)
		}
	}
	return out
}

// parseFirstRowKeys returns the object keys of the first row in a JSON response,
// in document order, preserving the original xlsx column sequence.
//
// Two response shapes are supported:
//   - {"data": [{...}, ...]}  -- wrapped format (LibreOffice service default)
//   - [{...}, ...]            -- plain JSON array
//
// Keys are returned unsanitised; call sanitizeColumnName on each before use.
func parseFirstRowKeys(body []byte) []string {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}

	switch delim {
	case '[':
		// Plain array — first element is the first data row.
		return readFirstObjectKeys(dec)
	case '{':
		// Wrapped format — scan object until we find the "data" key.
		for dec.More() {
			tok, err := dec.Token()
			if err != nil {
				return nil
			}
			key, ok := tok.(string)
			if !ok {
				return nil
			}
			if key != "data" {
				var skip json.RawMessage
				if err := dec.Decode(&skip); err != nil {
					return nil
				}
				continue
			}
			// Found "data" — expect '['.
			tok, err = dec.Token()
			if err != nil {
				return nil
			}
			if tok != json.Delim('[') {
				return nil
			}
			return readFirstObjectKeys(dec)
		}
	}
	return nil
}

// readFirstObjectKeys reads the key names of the next JSON object from dec,
// in document order. dec must be positioned just after the opening '[' of the
// containing array.
func readFirstObjectKeys(dec *json.Decoder) []string {
	if !dec.More() {
		return nil
	}
	tok, err := dec.Token()
	if err != nil {
		return nil
	}
	if tok != json.Delim('{') {
		return nil
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil
		}
		k, ok := tok.(string)
		if !ok {
			break
		}
		keys = append(keys, k)
		// Consume the value (may be any JSON type).
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil
		}
	}
	return keys
}
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
	if c.baseURL == "" {
		return nil, fmt.Errorf("libreoffice client: baseURL is not configured (set LIBREOFFICE_URL)")
	}

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
		var headers []string
		if len(wrapped.Headers) > 0 {
			// Use service-provided ordered header list.
			headers = make([]string, len(wrapped.Headers))
			for i, h := range wrapped.Headers {
				headers[i] = sanitizeColumnName(h)
			}
		} else {
			// Derive column order from the raw JSON body so the DuckDB table
			// columns match the original xlsx column sequence.
			raw := parseFirstRowKeys(body)
			headers = make([]string, len(raw))
			for i, h := range raw {
				headers[i] = sanitizeColumnName(h)
			}
		}
		rows := filterRefRows(sanitizeRows(wrapped.Data))
		return &ParseResult{Headers: headers, Rows: rows}, nil
	}

	// Fallback: plain JSON array of row objects
	var rows []map[string]interface{}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("failed to parse libreoffice response: %w", err)
	}
	rows = filterRefRows(sanitizeRows(rows))

	// Derive header list from the raw JSON body to preserve document order.
	raw := parseFirstRowKeys(body)
	headers := make([]string, len(raw))
	for i, h := range raw {
		headers[i] = sanitizeColumnName(h)
	}

	return &ParseResult{Headers: headers, Rows: rows}, nil
}
