package docai

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	documentai "cloud.google.com/go/documentai/apiv1"
	"cloud.google.com/go/documentai/apiv1/documentaipb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type Client struct {
	client      *documentai.DocumentProcessorClient
	projectID   string
	location    string
	processorID string
}

type ExtractedData struct {
	Text        string
	ExampleDate string // Just a placeholder, actual extraction depends on entities
	TotalAmount string
	Supplier    string
	Entities    map[string]string
}

func NewClient(ctx context.Context, projectID, location, processorID, credentialsPath string) (*Client, error) {
	opts := []option.ClientOption{}
	if credentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsPath))
	}

	// The endpoint must be set for the specific location if not us-central1 (though client usually handles it, explicit is safer for some regions)
	// For now, relying on default or let user configure via env if needed, but standard library usually handles regional endpoints well if location is passed in request.
	// Actually, for Document AI, we need to set the API endpoint if it's not global/us-neutral.
	if location != "us" && location != "us-central1" {
		opts = append(opts, option.WithEndpoint(fmt.Sprintf("%s-documentai.googleapis.com:443", location)))
	}

	c, err := documentai.NewDocumentProcessorClient(ctx, opts...)
	if err != nil {
		slog.Error("Failed to create Document AI client", "error", err)
		return nil, fmt.Errorf("failed to create document ai client: %w", err)
	}

	return &Client{
		client:      c,
		projectID:   projectID,
		location:    location,
		processorID: processorID,
	}, nil
}

func (c *Client) ProcessDocument(ctx context.Context, fileContent []byte, mimeType string) (*documentaipb.Document, error) {
	if len(fileContent) == 0 {
		slog.Error("Document AI: attempt to process empty file content")
		return nil, fmt.Errorf("file content is empty")
	}

	name := fmt.Sprintf("projects/%s/locations/%s/processors/%s", c.projectID, c.location, c.processorID)
	slog.Debug("Preparing Document AI request", "resource_name", name, "mime_type", mimeType, "content_size", len(fileContent))

	req := &documentaipb.ProcessRequest{
		Name: name,
		FieldMask: &fieldmaskpb.FieldMask{
			Paths: []string{"text", "entities"},
		},
		Source: &documentaipb.ProcessRequest_RawDocument{
			RawDocument: &documentaipb.RawDocument{
				Content:  fileContent,
				MimeType: mimeType,
			},
		},
	}

	slog.Info("Sending document to Google Cloud Document AI", "processor_id", c.processorID)
	resp, err := c.client.ProcessDocument(ctx, req)
	if err != nil {
		slog.Error("Document AI processing failed", "error", err)
		return nil, fmt.Errorf("failed to process document: %w", err)
	}

	slog.Info("Document AI processing completed successfully")
	return resp.Document, nil
}

func (c *Client) ExtractData(doc *documentaipb.Document) *ExtractedData {
	data := &ExtractedData{
		Text:     doc.Text,
		Entities: make(map[string]string),
	}

	// Iterate specific entities for Invoice Parser
	for _, entity := range doc.Entities {
		// Normalize type or just store raw
		// Common invoice types: invoice_date, total_amount, supplier_name, currency...
		key := entity.Type
		val := entity.MentionText
		if val == "" && entity.TextAnchor != nil && entity.TextAnchor.Content != "" {
			val = entity.TextAnchor.Content // Fallback if MentionText is empty
		}

		slog.Debug("Extracted entity", "type", key, "value", val)

		// Normalize key if necessary (e.g. remove "invoice_" prefix)
		data.Entities[key] = val

		// Quick access fields
		switch key {
		case "invoice_date":
			data.ExampleDate = val
		case "total_amount":
			data.TotalAmount = val
			// Check for normalized value if available
			if entity.NormalizedValue != nil {
				data.TotalAmount = entity.NormalizedValue.Text
				slog.Debug("Using normalized amount", "amount", data.TotalAmount)
			}
			//a number with exactly two decimals
			parts := strings.Split(data.TotalAmount, ".")
			switch len(parts) {
			case 1:
				data.TotalAmount += ".00"
			case 2:
				if len(parts[1]) > 2 {
					parts[1] = parts[1][:2]
				} else if len(parts[1]) < 2 {
					for len(parts[1]) < 2 {
						parts[1] += "0"
					}
				}
				data.TotalAmount = strings.Join(parts, ".")
			}

		case "supplier_name":
			data.Supplier = val
		}
	}

	slog.Info("Entity extraction completed", "entities_count", len(doc.Entities))
	return data
}

func (c *Client) Close() error {
	return c.client.Close()
}
