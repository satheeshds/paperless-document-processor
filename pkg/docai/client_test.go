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

func createLineItemProperty(typeStr, mentionText, content string, normalizedValue *documentaipb.Document_Entity_NormalizedValue) *documentaipb.Document_Entity {
	return createEntity(typeStr, mentionText, content, normalizedValue)
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

func TestExtractData_LineItems(t *testing.T) {
	doc := &documentaipb.Document{
		Entities: []*documentaipb.Document_Entity{
			{
				Type: "line_item",
				Properties: []*documentaipb.Document_Entity{
					createLineItemProperty("description", "Consulting services", "Consulting services", nil),
					createLineItemProperty("quantity", "2", "2", &documentaipb.Document_Entity_NormalizedValue{Text: "2"}),
					createLineItemProperty("unit_price", "$50.25", "$50.25", &documentaipb.Document_Entity_NormalizedValue{Text: "50.25"}),
					createLineItemProperty("amount", "$100.50", "$100.50", &documentaipb.Document_Entity_NormalizedValue{Text: "100.50"}),
				},
			},
		},
	}

	client := &Client{}
	extracted := client.ExtractData(doc)

	if len(extracted.LineItems) != 1 {
		t.Fatalf("Expected 1 line item, got %d", len(extracted.LineItems))
	}

	item := extracted.LineItems[0]
	if item.Description != "Consulting services" {
		t.Errorf("Expected description 'Consulting services', got '%s'", item.Description)
	}
	if item.Quantity != "2" {
		t.Errorf("Expected quantity '2', got '%s'", item.Quantity)
	}
	if item.UnitPrice != "50.25" {
		t.Errorf("Expected unit price '50.25', got '%s'", item.UnitPrice)
	}
	if item.Amount != "100.50" {
		t.Errorf("Expected amount '100.50', got '%s'", item.Amount)
	}
}
