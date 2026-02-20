package tika

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	baseURL string
	client  *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{},
	}
}

func (c *Client) Parse(content []byte) (string, error) {
	if len(content) == 0 {
		return "", fmt.Errorf("content is empty")
	}

	// Use recursive metadata endpoint to get XHTML inside JSON
	req, err := http.NewRequest("PUT", c.baseURL+"/rmeta/xhtml", bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("tika returned status %d: %s", resp.StatusCode, string(body))
	}

	var results []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return "", fmt.Errorf("failed to decode Tika JSON: %w", err)
	}

	if len(results) == 0 {
		return "", fmt.Errorf("tika returned no results")
	}

	// The first object contains the main content in XHTML
	if content, ok := results[0]["X-TIKA:content"].(string); ok {
		return content, nil
	}

	return "", fmt.Errorf("X-TIKA:content not found in Tika response")
}
