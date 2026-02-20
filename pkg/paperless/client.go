package paperless

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
)

type Client struct {
	baseURL string
	token   string
	client  *http.Client
}

type Document struct {
	ID               int                   `json:"id"`
	Title            string                `json:"title"`
	Correspondent    *int                  `json:"correspondent"`
	Created          string                `json:"created"`
	Modified         string                `json:"modified"`
	Added            string                `json:"added"`
	OriginalFileName string                `json:"original_file_name"`
	ArchivedFileName string                `json:"archived_file_name"`
	CustomFields     []CustomFieldInstance `json:"custom_fields"`
	Tags             []int                 `json:"tags"`
}

type Metadata struct {
	MediaFilename        string `json:"media_filename"`
	OriginalChecksum     string `json:"original_checksum"`
	OriginalMimeType     string `json:"original_mime_type"`
	HasArchiveVersion    bool   `json:"has_archive_version"`
	ArchiveChecksum      string `json:"archive_checksum"`
	ArchiveMediaFilename string `json:"archive_media_filename"`
	OriginalFileName     string `json:"original_file_name"`
}

type CustomFieldInstance struct {
	Field int         `json:"field"`
	Value interface{} `json:"value"`
}

type CustomField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	DataType string `json:"data_type"` // e.g., "date", "monetary", "string"
}

type Correspondent struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type Tag struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type PaginatedResponse[T any] struct {
	Count    int    `json:"count"`
	Next     string `json:"next"`
	Previous string `json:"previous"`
	Results  []T    `json:"results"`
}

func NewClient(baseURL, token string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		client:  &http.Client{},
	}
}

func (c *Client) request(method, path string, body interface{}) (*http.Response, error) {
	u := fmt.Sprintf("%s/api/%s", c.baseURL, path)
	slog.Debug("Paperless API request", "method", method, "url", u)

	var buf io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			slog.Error("Failed to marshal request body", "error", err)
			return nil, err
		}
		buf = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequest(method, u, buf)
	if err != nil {
		slog.Error("Failed to create request", "error", err)
		return nil, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Token %s", c.token))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	slog.Debug("Executing Paperless request", "method", method, "url", u)
	resp, err := c.client.Do(req)
	if err != nil {
		slog.Error("API request execution failed", "method", method, "url", u, "error", err)
		return nil, err
	}

	slog.Debug("Paperless response received", "method", method, "url", u, "status", resp.Status)

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		err := fmt.Errorf("api request failed: %s %s: status %d body: %s", method, u, resp.StatusCode, string(respBody))
		slog.Error("Paperless API error", "status", resp.StatusCode, "method", method, "url", u, "response", string(respBody))
		return nil, err
	}

	return resp, nil
}

func (c *Client) GetDocument(id int) (*Document, error) {
	slog.Info("Fetching document from Paperless", "id", id)
	resp, err := c.request("GET", fmt.Sprintf("documents/%d/", id), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var doc Document
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		slog.Error("Failed to decode document response", "id", id, "error", err)
		return nil, err
	}
	return &doc, nil
}

func (c *Client) GetMetadata(id int) (*Metadata, error) {
	slog.Debug("Fetching document metadata", "id", id)
	resp, err := c.request("GET", fmt.Sprintf("documents/%d/metadata/", id), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var meta Metadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		slog.Error("Failed to decode metadata response", "id", id, "error", err)
		return nil, err
	}
	return &meta, nil
}

func (c *Client) DownloadDocument(id int, original bool) ([]byte, error) {
	slog.Info("Downloading document content", "id", id, "original", original)
	u := fmt.Sprintf("%s/api/documents/%d/download/", c.baseURL, id)
	if original {
		u += "?original=true"
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		slog.Error("Failed to create download request", "id", id, "error", err)
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", c.token))

	resp, err := c.client.Do(req)
	if err != nil {
		slog.Error("Download request error", "id", id, "error", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		slog.Error("Failed to download document", "id", id, "status", resp.StatusCode)
		return nil, fmt.Errorf("failed to download document: status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("Failed to read response body", "id", id, "error", err)
		return nil, err
	}

	contentLen := len(data)
	slog.Info("Download complete", "id", id, "size_bytes", contentLen)

	if contentLen == 0 {
		slog.Error("Downloaded document is empty", "id", id)
		return nil, fmt.Errorf("downloaded document is empty")
	}

	if contentLen > 16 {
		slog.Debug("File signature (hex)", "id", id, "hex", hex.EncodeToString(data[:16]), "prefix", string(data[:16]))
	} else {
		slog.Debug("File signature (hex)", "id", id, "hex", hex.EncodeToString(data))
	}

	return data, nil
}

func (c *Client) GetCustomFields() ([]CustomField, error) {
	var allFields []CustomField
	nextURL := "custom_fields/"

	for nextURL != "" {
		resp, err := c.request("GET", nextURL, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var page PaginatedResponse[CustomField]
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, err
		}

		allFields = append(allFields, page.Results...)

		if page.Next != "" {
			if strings.Contains(page.Next, "/api/") {
				parts := strings.Split(page.Next, "/api/")
				if len(parts) > 1 {
					nextURL = parts[1]
				} else {
					nextURL = ""
				}
			} else {
				nextURL = ""
			}
		} else {
			nextURL = ""
		}
	}
	return allFields, nil
}

func (c *Client) GetTags() ([]Tag, error) {
	var allTags []Tag
	nextURL := "tags/"

	for nextURL != "" {
		resp, err := c.request("GET", nextURL, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var page PaginatedResponse[Tag]
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, err
		}

		allTags = append(allTags, page.Results...)

		if page.Next != "" {
			if strings.Contains(page.Next, "/api/") {
				parts := strings.Split(page.Next, "/api/")
				if len(parts) > 1 {
					nextURL = parts[1]
				} else {
					nextURL = ""
				}
			} else {
				nextURL = ""
			}
		} else {
			nextURL = ""
		}
	}
	return allTags, nil
}

func (c *Client) GetCorrespondent(name string) (*Correspondent, error) {
	// Search by name (slug search is better if we can normalize, but name search via list with query param)
	// paperless api allows filtering correspondents? yes: /api/correspondents/?name__icontains=...
	// but exact match is harder. Let's fetch all (cached maybe?) or search.
	// Search is safer for now.
	q := url.Values{}
	q.Set("name__iexact", name) // Case insensitive exact match
	path := fmt.Sprintf("correspondents/?%s", q.Encode())

	resp, err := c.request("GET", path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var page PaginatedResponse[Correspondent]
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, err
	}

	if len(page.Results) > 0 {
		return &page.Results[0], nil
	}
	return nil, nil
}

func (c *Client) CreateCorrespondent(name string) (*Correspondent, error) {
	slog.Info("Creating correspondent in Paperless", "name", name)
	body := map[string]string{"name": name, "match": "", "matching_algorithm": "1", "is_insensitive": "true"}
	resp, err := c.request("POST", "correspondents/", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var corr Correspondent
	if err := json.NewDecoder(resp.Body).Decode(&corr); err != nil {
		slog.Error("Failed to decode correspondent response", "name", name, "error", err)
		return nil, err
	}
	return &corr, nil
}

type DocumentUpdate struct {
	Title         *string               `json:"title,omitempty"`
	Content       *string               `json:"content,omitempty"`
	Correspondent *int                  `json:"correspondent,omitempty"`
	CustomFields  []CustomFieldInstance `json:"custom_fields,omitempty"`
}

func (c *Client) UpdateDocument(id int, update DocumentUpdate) error {
	slog.Info("Updating document metadata", "id", id)
	resp, err := c.request("PATCH", fmt.Sprintf("documents/%d/", id), update)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	slog.Info("Successfully updated document in Paperless", "id", id)
	return nil
}
