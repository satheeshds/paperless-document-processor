package paperless

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNewClient_TrimsTrailingSlash verifies that the base URL is stored without
// a trailing slash.
func TestNewClient_TrimsTrailingSlash(t *testing.T) {
	c := NewClient("http://paperless:8000/", "tok")
	if c.baseURL != "http://paperless:8000" {
		t.Errorf("baseURL = %q, want trailing slash removed", c.baseURL)
	}
}

// TestGetDocument_Success verifies that GetDocument decodes the response body.
func TestGetDocument_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/documents/42/" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Token mytoken" {
			t.Errorf("unexpected auth header: %s", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Document{ID: 42, Title: "Invoice Jan"})
	}))
	defer server.Close()

	c := NewClient(server.URL, "mytoken")
	doc, err := c.GetDocument(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.ID != 42 {
		t.Errorf("ID = %d, want 42", doc.ID)
	}
	if doc.Title != "Invoice Jan" {
		t.Errorf("Title = %q, want \"Invoice Jan\"", doc.Title)
	}
}

// TestGetDocument_ErrorStatus verifies that a 4xx response returns an error.
func TestGetDocument_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	_, err := c.GetDocument(99)
	if err == nil {
		t.Fatal("expected error for 404 response, got nil")
	}
}

// TestGetMetadata_Success verifies that GetMetadata decodes the metadata response.
func TestGetMetadata_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/documents/5/metadata/" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Metadata{
			MediaFilename:     "originals/5/invoice.pdf",
			OriginalMimeType:  "application/pdf",
			HasArchiveVersion: true,
		})
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	meta, err := c.GetMetadata(5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.MediaFilename != "originals/5/invoice.pdf" {
		t.Errorf("MediaFilename = %q, want %q", meta.MediaFilename, "originals/5/invoice.pdf")
	}
	if !meta.HasArchiveVersion {
		t.Error("expected HasArchiveVersion true")
	}
}

// TestDownloadDocument_Success verifies downloading of document content.
func TestDownloadDocument_Success(t *testing.T) {
	fakeContent := []byte("fake-pdf-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/documents/7/download/" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write(fakeContent)
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	data, err := c.DownloadDocument(7, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != string(fakeContent) {
		t.Errorf("got %q, want %q", string(data), string(fakeContent))
	}
}

// TestDownloadDocument_Original verifies that original=true is forwarded as
// a query parameter.
func TestDownloadDocument_Original(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("original") != "true" {
			t.Errorf("expected original=true query param, got %q", r.URL.Query().Get("original"))
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("original-pdf-bytes"))
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	_, err := c.DownloadDocument(7, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestDownloadDocument_EmptyBody verifies that an empty response body returns
// an error.
func TestDownloadDocument_EmptyBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Write nothing
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	_, err := c.DownloadDocument(7, false)
	if err == nil {
		t.Fatal("expected error for empty body, got nil")
	}
}

// TestDownloadDocument_NonOKStatus verifies that a non-200 status returns an error.
func TestDownloadDocument_NonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	_, err := c.DownloadDocument(7, false)
	if err == nil {
		t.Fatal("expected error for non-200 status, got nil")
	}
}

// TestGetCustomFields_Success verifies that custom fields are collected from a
// single-page response.
func TestGetCustomFields_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/custom_fields") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaginatedResponse[CustomField]{
			Count: 2,
			Results: []CustomField{
				{ID: 1, Name: "Invoice Date", DataType: "date"},
				{ID: 2, Name: "Total", DataType: "monetary"},
			},
		})
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	fields, err := c.GetCustomFields()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("expected 2 custom fields, got %d", len(fields))
	}
	if fields[0].Name != "Invoice Date" {
		t.Errorf("expected first field name 'Invoice Date', got %q", fields[0].Name)
	}
}

// TestGetCustomFields_Pagination verifies that GetCustomFields follows the Next
// URL across multiple pages.
func TestGetCustomFields_Pagination(t *testing.T) {
	page := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		page++
		if page == 1 {
			// Include a Next URL pointing to the second page, using the /api/ prefix
			// so the client can extract the relative path.
			json.NewEncoder(w).Encode(PaginatedResponse[CustomField]{
				Count: 3,
				Next:  r.Host + "/api/custom_fields/?page=2",
				Results: []CustomField{
					{ID: 1, Name: "Field A"},
				},
			})
		} else {
			json.NewEncoder(w).Encode(PaginatedResponse[CustomField]{
				Count: 3,
				Results: []CustomField{
					{ID: 2, Name: "Field B"},
					{ID: 3, Name: "Field C"},
				},
			})
		}
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	fields, err := c.GetCustomFields()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 3 {
		t.Errorf("expected 3 fields across two pages, got %d", len(fields))
	}
}

// TestGetTags_Success verifies that tags are retrieved successfully.
func TestGetTags_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaginatedResponse[Tag]{
			Count:   2,
			Results: []Tag{{ID: 10, Name: "bills"}, {ID: 11, Name: "invoices"}},
		})
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	tags, err := c.GetTags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tags))
	}
	if tags[0].Name != "bills" {
		t.Errorf("expected first tag 'bills', got %q", tags[0].Name)
	}
}

// TestGetCorrespondent_Found verifies the happy path where a correspondent is
// returned from the search endpoint.
func TestGetCorrespondent_Found(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/correspondents/") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaginatedResponse[Correspondent]{
			Count:   1,
			Results: []Correspondent{{ID: 5, Name: "Acme Corp", Slug: "acme-corp"}},
		})
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	corr, err := c.GetCorrespondent("Acme Corp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if corr == nil {
		t.Fatal("expected correspondent, got nil")
	}
	if corr.ID != 5 {
		t.Errorf("ID = %d, want 5", corr.ID)
	}
}

// TestGetCorrespondent_NotFound verifies that nil is returned when no results
// are found.
func TestGetCorrespondent_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PaginatedResponse[Correspondent]{Count: 0, Results: []Correspondent{}})
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	corr, err := c.GetCorrespondent("Unknown Vendor")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if corr != nil {
		t.Errorf("expected nil correspondent, got %+v", corr)
	}
}

// TestCreateCorrespondent_Success verifies that the request is sent correctly
// and the response is decoded.
func TestCreateCorrespondent_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/correspondents/" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(Correspondent{ID: 9, Name: "New Vendor", Slug: "new-vendor"})
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	corr, err := c.CreateCorrespondent("New Vendor")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if corr.ID != 9 {
		t.Errorf("ID = %d, want 9", corr.ID)
	}
	if corr.Name != "New Vendor" {
		t.Errorf("Name = %q, want \"New Vendor\"", corr.Name)
	}
}

// TestUpdateDocument_Success verifies that PATCH is sent and no error is returned.
func TestUpdateDocument_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		if r.URL.Path != "/api/documents/42/" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var update DocumentUpdate
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Document{ID: 42})
	}))
	defer server.Close()

	title := "Updated Title"
	c := NewClient(server.URL, "tok")
	err := c.UpdateDocument(42, DocumentUpdate{Title: &title})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestUpdateDocument_ErrorStatus verifies that a 4xx response is surfaced as an
// error.
func TestUpdateDocument_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	c := NewClient(server.URL, "tok")
	title := "Title"
	err := c.UpdateDocument(42, DocumentUpdate{Title: &title})
	if err == nil {
		t.Fatal("expected error for 400 response, got nil")
	}
}
