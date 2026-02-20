package docai

import (
	"testing"

	"cloud.google.com/go/documentai/apiv1/documentaipb"
)

// Helper to create a fake document entity
func createEntity(typeStr, mentionText, content string, normalizedValue *documentaipb.Document_Entity_NormalizedValue) *documentaipb.Document_Entity {
	return &documentaipb.Document_Entity{
		Type:            typeStr,
		MentionText:     mentionText,
		TextAnchor:      &documentaipb.Document_TextAnchor{Content: content},
		NormalizedValue: normalizedValue,
	}
}

func TestExtractData(t *testing.T) {
	// Setup a mock Document
	doc := &documentaipb.Document{
		Text: "Invoice #123\nDate: 2023-10-25\nTotal: $100.50\nSupplier: Acme Corp",
		Entities: []*documentaipb.Document_Entity{
			createEntity("invoice_date", "2023-10-25", "2023-10-25", nil),
			createEntity("total_amount", "$100.50", "$100.50", &documentaipb.Document_Entity_NormalizedValue{Text: "100.50"}),
			createEntity("supplier_name", "Acme Corp", "Acme Corp", nil),
			createEntity("currency", "$", "$", &documentaipb.Document_Entity_NormalizedValue{Text: "USD"}),
		},
	}

	// Because `ExtractData` is a method on *Client, but doesn't use the client state,
	// we can instantiate a dummy client or refactor ExtractData to be a function.
	// Current implementation: func (c *Client) ExtractData(...)
	// It doesn't use `c` inside. So we can use a nil client or empty client.
	client := &Client{}

	extracted := client.ExtractData(doc)

	if extracted.ExampleDate != "2023-10-25" {
		t.Errorf("Expected date '2023-10-25', got '%s'", extracted.ExampleDate)
	}

	if extracted.TotalAmount != "100.50" {
		t.Errorf("Expected total '100.50', got '%s'", extracted.TotalAmount)
	}

	if extracted.Supplier != "Acme Corp" {
		t.Errorf("Expected supplier 'Acme Corp', got '%s'", extracted.Supplier)
	}

	if val, ok := extracted.Entities["currency"]; !ok || val != "$" {
		t.Errorf("Expected currency entity '$', got '%v'", val)
	}
}

func TestExtractData_Fallback(t *testing.T) {
	// Test fallback to TextAnchor content if MentionText is empty
	doc := &documentaipb.Document{
		Entities: []*documentaipb.Document_Entity{
			{
				Type:        "invoice_date",
				MentionText: "", // Empty
				TextAnchor:  &documentaipb.Document_TextAnchor{Content: "2023-01-01"},
			},
		},
	}

	client := &Client{}
	extracted := client.ExtractData(doc)

	if extracted.ExampleDate != "2023-01-01" {
		t.Errorf("Expected date '2023-01-01', got '%s'", extracted.ExampleDate)
	}
}
