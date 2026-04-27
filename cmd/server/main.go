package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"paperless-document-processor/config"
	"paperless-document-processor/pkg/portal"
	"paperless-document-processor/pkg/docai"
	"paperless-document-processor/pkg/excel"
	"paperless-document-processor/pkg/libreoffice"
	"paperless-document-processor/pkg/paperless"
	"paperless-document-processor/pkg/storage"
	"paperless-document-processor/pkg/tika"

	"github.com/gabriel-vasile/mimetype"
)

type Server struct {
	cfg               *config.Config
	paperlessClient   *paperless.Client
	docAIClient       *docai.Client
	tikaClient        *tika.Client        // nil if not configured
	libreOfficeClient *libreoffice.Client // nil if not configured
	customFields      map[string]int      // Name -> ID
	tagIDs            map[string]int      // Name -> ID (e.g., "Swiggy" -> 3)
	tagIDsMu          sync.RWMutex        // protects tagIDs for concurrent handler access
	duckDBConfigs     map[int]config.PlatformConfig
}

type BillRequest struct {
	DocURL string `json:"doc_url"`
}

type PayoutRequest struct {
	DocURL string `json:"doc_url"`
}

type BankStatementRequest struct {
	DocURL string `json:"doc_url"`
}

func main() {
	// 1. Load Config
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 2. Setup Logger
	var lvl slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	// 3. Init DB — run schema migrations for all tenants at startup, then
	// per-request connections are opened via OpenWithTenant.
	storage.ValidateConfig(cfg.Nexus)
	if err := storage.MigrateAllTenants(cfg.Nexus); err != nil {
		slog.Warn("Startup migration failed for one or more tenants (will be retried on next startup)", "error", err)
	}

	// 3. Init Clients
	pClient := paperless.NewClient(cfg.PaperlessURL, cfg.PaperlessUsername, cfg.PaperlessPassword)

	ctx := context.Background()
	dClient, err := docai.NewClient(ctx, cfg.GoogleProjectID, cfg.GoogleLocation, cfg.DocumentAIProcessorID, cfg.GoogleCredentialsPath)
	if err != nil {
		slog.Error("Failed to init DocAI client", "error", err)
		os.Exit(1)
	}
	defer dClient.Close()

	// Init Portal client (optional)
	if cfg.PortalURL != "" {
		slog.Info("Portal integration enabled", "url", cfg.PortalURL)
	} else {
		slog.Info("Portal integration disabled (PORTAL_URL not set)")
	}

	// Init LibreOffice parser client (optional)
	var loClient *libreoffice.Client
	if cfg.LibreOfficeURL != "" {
		loClient = libreoffice.NewClient(cfg.LibreOfficeURL, cfg.LibreOfficeDataPath)
		slog.Info("LibreOffice parser integration configured", "url", cfg.LibreOfficeURL, "data_path", cfg.LibreOfficeDataPath)
	} else {
		slog.Info("LibreOffice parser integration disabled (LIBREOFFICE_URL not set)")
	}

	srv := &Server{
		cfg:               cfg,
		paperlessClient:   pClient,
		docAIClient:       dClient,
		tikaClient:        tika.NewClient(cfg.TikaURL),
		libreOfficeClient: loClient,
		customFields:      make(map[string]int),
		tagIDs:            make(map[string]int),
		duckDBConfigs:     make(map[int]config.PlatformConfig),
	}

	// 4. Fetch Custom Fields (Retry policy could be added)
	slog.Info("Fetching custom fields from Paperless...")
	fields, err := pClient.GetCustomFields()
	if err != nil {
		slog.Warn("Failed to fetch custom fields. Custom field updates will be skipped.", "error", err)
	} else {
		for _, f := range fields {
			srv.customFields[f.Name] = f.ID
		}
		slog.Info("Loaded custom fields", "count", len(srv.customFields))
	}

	// 5. Fetch Tags and Setup DuckDB Configs
	slog.Info("Fetching tags from Paperless...")
	tags, err := pClient.GetTags()
	if err != nil {
		slog.Warn("Failed to fetch tags. Dynamic DuckDB config will be limited.", "error", err)
	} else {
		for _, t := range tags {
			srv.tagIDs[t.Name] = t.ID
		}
		slog.Info("Loaded tags", "count", len(srv.tagIDs))

		// Load external payout configs if path is set
		if cfg.PayoutConfigPath != "" {
			slog.Info("Loading payout configurations from file", "path", cfg.PayoutConfigPath)
			data, err := os.ReadFile(cfg.PayoutConfigPath)
			if err != nil {
				slog.Error("Failed to read payout config file", "path", cfg.PayoutConfigPath, "error", err)
			} else {
				var pConfigs config.PayoutConfigs
				if err := json.Unmarshal(data, &pConfigs); err != nil {
					slog.Error("Failed to parse payout config JSON", "error", err)
				} else {
					for platform, options := range pConfigs.Platforms {
						if id, ok := srv.tagIDs[platform]; ok {
							srv.duckDBConfigs[id] = options
							slog.Info("Configured platform via JSON", "platform", platform, "tag_id", id)
						} else {
							slog.Warn("Platform in config not found in Paperless tags", "platform", platform)
						}
					}
				}
			}
		} else {
			slog.Info("No PAYOUT_EXCEL_DUCKDB_CONFIG_PATH set, using defaults")
			// Fallback to hardcoded defaults for Swiggy/Zomato
			// if id, ok := srv.tagIDs["swiggy"]; ok {
			// 	srv.duckDBConfigs[id] = map[string]interface{}{
			// 		"header":        true,
			// 		"sheet":         "Order Level",
			// 		"range":         "A3:AR",
			// 		"stop_at_empty": true,
			// 	}
			// }
			// if id, ok := srv.tagIDs["zomato"]; ok {
			// 	srv.duckDBConfigs[id] = map[string]interface{}{
			// 		"header":      true,
			// 		"sheet":       "Payout Breakup",
			// 		"all_varchar": true,
			// 	}
			// }
		}
	}

	// 6. Start Server
	http.HandleFunc("POST /bills", srv.handleBills)
	http.HandleFunc("POST /payouts", srv.handlePayouts)
	http.HandleFunc("POST /bank-statements", srv.handleBankStatements)
	slog.Info("Starting server", "port", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, nil); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}

// getOrCreateTag returns the Paperless tag ID for the given name, creating the
// tag if it does not already exist. Results are cached in srv.tagIDs.
// It is safe to call concurrently from multiple goroutines.
func (s *Server) getOrCreateTag(name string) (int, error) {
	// Fast path: read lock to check the cache.
	s.tagIDsMu.RLock()
	id, ok := s.tagIDs[name]
	s.tagIDsMu.RUnlock()
	if ok {
		return id, nil
	}

	// Slow path: create the tag in Paperless, then update the cache.
	tag, err := s.paperlessClient.CreateTag(name)
	if err != nil {
		return 0, err
	}

	s.tagIDsMu.Lock()
	s.tagIDs[name] = tag.ID
	s.tagIDsMu.Unlock()

	return tag.ID, nil
}

// applyErrorTags merges the given tag names into the document's existing tags
// and PATCHes the document in Paperless.
func (s *Server) applyErrorTags(docID int, existingTagIDs []int, tagNames ...string) {
	tagSet := make(map[int]struct{}, len(existingTagIDs))
	for _, id := range existingTagIDs {
		tagSet[id] = struct{}{}
	}
	for _, name := range tagNames {
		id, err := s.getOrCreateTag(name)
		if err != nil {
			slog.Warn("Failed to get/create error tag", "tag", name, "error", err)
			continue
		}
		tagSet[id] = struct{}{}
	}
	merged := make([]int, 0, len(tagSet))
	for id := range tagSet {
		merged = append(merged, id)
	}
	if err := s.paperlessClient.UpdateDocument(docID, paperless.DocumentUpdate{Tags: merged}); err != nil {
		slog.Warn("Failed to apply error tags to document", "document_id", docID, "error", err)
	}
}

// extractDocIDFromURL parses the numeric document ID from a Paperless document
// URL of the form "http://host/documents/73/" and returns it.
func extractDocIDFromURL(docURL string) (int, error) {
	trimmed := strings.TrimSuffix(docURL, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid doc_url format: %q", docURL)
	}
	id, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return 0, fmt.Errorf("invalid document ID in URL %q: %w", docURL, err)
	}
	return id, nil
}

// openTenantResources resolves the tenant for docID, rotates service-account
// credentials exactly once, opens a per-request Nexus DB connection, and (when
// PortalURL is configured) creates an portal.Client using the same
// rotated service_id / service_api_key as HTTP Basic Auth credentials.
// If the tenant cannot be resolved or the DB cannot be opened the method writes
// the appropriate HTTP response and returns (nil, nil, false).
func (s *Server) openTenantResources(w http.ResponseWriter, docID int) (*storage.DB, *portal.Client, bool) {
	tenantID, _, err := s.resolveTenantID(docID)
	if err != nil {
		slog.Error("Failed to resolve tenant", "document_id", docID, "error", err)
		http.Error(w, "Failed to fetch document from Paperless", http.StatusInternalServerError)
		return nil, nil, false
	}
	if tenantID == "" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Cannot process: tenant not set"))
		return nil, nil, false
	}
	db, acClient, err := storage.OpenWithTenantAndPortal(s.cfg.Nexus, tenantID, s.cfg.PortalURL)
	if err != nil {
		slog.Error("Failed to open tenant resources", "tenant_id", tenantID, "document_id", docID, "error", err)
		http.Error(w, "Failed to open tenant database", http.StatusInternalServerError)
		return nil, nil, false
	}
	return db, acClient, true
}

// resolveTenantID fetches the document from Paperless and returns the value of
// the "tenant" custom field. If the field is absent or empty it applies error
// tags to the document and returns an empty tenantID (no error is returned in
// that case – the caller should treat empty tenantID as a non-fatal skip).
func (s *Server) resolveTenantID(docID int) (tenantID string, doc *paperless.Document, err error) {
	doc, err = s.paperlessClient.GetDocument(docID)
	if err != nil {
		return "", nil, fmt.Errorf("fetching document %d: %w", docID, err)
	}

	tenantFieldID, ok := s.customFields["tenant"]
	if !ok {
		slog.Warn("'tenant' custom field not found in Paperless, cannot determine tenant", "document_id", docID)
		s.applyErrorTags(docID, doc.Tags, "status:error", "err:missing_tenant")
		return "", doc, nil
	}

	for _, cf := range doc.CustomFields {
		if cf.Field == tenantFieldID {
			if v, ok := cf.Value.(string); ok && v != "" {
				return v, doc, nil
			}
			break
		}
	}

	slog.Warn("'tenant' custom field is missing or empty on document", "document_id", docID)
	s.applyErrorTags(docID, doc.Tags, "status:error", "err:missing_tenant")
	return "", doc, nil
}

func (s *Server) handleBills(w http.ResponseWriter, r *http.Request) {

	var req BillRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode bill request", "error", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	docID, err := extractDocIDFromURL(req.DocURL)
	if err != nil {
		slog.Error("Invalid document URL in bill request", "url", req.DocURL, "error", err)
		http.Error(w, "Invalid document URL", http.StatusBadRequest)
		return
	}

	slog.Info("Received bill request", "doc_url", req.DocURL, "document_id", docID)

	db, acClient, ok := s.openTenantResources(w, docID)
	if !ok {
		return
	}

	// Run processing asynchronously
	go s.processBill(docID, req, db, acClient)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Processing started"))
}

func (s *Server) processBill(docID int, req BillRequest, db *storage.DB, acClient *portal.Client) {
	defer db.Close()
	slog.Info("Starting processing", "document_id", docID)

	// 1. Get Metadata
	doc, err := s.paperlessClient.GetDocument(docID)
	if err != nil {
		slog.Error("Error getting document", "document_id", docID, "error", err)
		return
	}

	// 2. Download Content
	content, err := s.paperlessClient.DownloadDocument(docID, false)
	if err != nil {
		slog.Error("Error downloading content", "document_id", docID, "error", err)
		return
	}

	// 3. Process with DocAI
	mtype := mimetype.Detect(content)
	mimeType := mtype.String()
	slog.Info("Detected MIME type", "document_id", docID, "mimetype", mimeType, "extension", mtype.Extension())

	slog.Info("Sending to Document AI", "document_id", docID, "mime_type", mimeType)
	aiDoc, err := s.docAIClient.ProcessDocument(context.Background(), "", content, mimeType)
	if err != nil {
		slog.Error("DocAI error", "document_id", docID, "error", err)
		return
	}

	extracted := s.docAIClient.ExtractData(aiDoc)
	slog.Info("Extracted data", "document_id", docID, "supplier", extracted.Supplier, "date", extracted.ExampleDate, "total", extracted.TotalAmount)

	// 4. Save to DB
	// Serialize Extracted + Full Response?
	// We'll just save the extracted for now + raw JSON if we had it (aiDoc is protobuf)
	// For "raw_ocr_data", we can marshal aiDoc to JSON.
	rawJSON, _ := json.Marshal(aiDoc.Entities)

	totalAmount, _ := strconv.ParseFloat(extracted.TotalAmount, 64) // weak parsing, clean up usually needed (remove currency symbols)

	dbDoc := &storage.ProcessedDocument{
		PaperlessID:   docID,
		Filename:      doc.OriginalFileName,
		Supplier:      extracted.Supplier,
		Date:          extracted.ExampleDate,
		TotalAmount:   totalAmount,
		RawOCRData:    string(rawJSON),
		ExtractedText: extracted.Text,
	}

	if err := db.SaveDocument(dbDoc); err != nil {
		slog.Error("DB Save error", "document_id", docID, "error", err)
		// Continue anyway? Yes.
	}

	// 4b. Create Bill in Portal (optional)
	if acClient != nil {
		s.createLocalBill(docID, extracted, doc, req, acClient)
	}

	// 5. Update Paperless
	updates := paperless.DocumentUpdate{}

	// Update Content
	if extracted.Text != "" {
		updates.Content = &extracted.Text
	}

	// Update Correspondent
	if extracted.Supplier != "" {
		corr, err := s.getOrCreateCorrespondent(extracted.Supplier)
		if err != nil {
			slog.Warn("Correspondent error", "document_id", docID, "error", err)
		} else {
			updates.Correspondent = &corr.ID
		}
	}

	// Update Title? Maybe "Supplier - Date"
	// if extracted.Supplier != "" && extracted.ExampleDate != "" {
	// 	newTitle := fmt.Sprintf("%s - %s", extracted.Supplier, extracted.ExampleDate)
	// 	updates.Title = &newTitle
	// 	// Note: paperless might complain if date format changes
	// }

	// Update Custom Fields
	var cfs []paperless.CustomFieldInstance

	if val, ok := extracted.Entities["invoice_date"]; ok && val != "" {
		if id, found := s.customFields["Invoice Date"]; found {
			cfs = append(cfs, paperless.CustomFieldInstance{Field: id, Value: val}) // Paperless expects YYYY-MM-DD usually
		}
	}
	if _, ok := extracted.Entities["total_amount"]; ok && extracted.TotalAmount != "" {
		if id, found := s.customFields["Total"]; found {
			cfs = append(cfs, paperless.CustomFieldInstance{Field: id, Value: extracted.TotalAmount})
		}
		if id, found := s.customFields["Amount"]; found {
			cfs = append(cfs, paperless.CustomFieldInstance{Field: id, Value: extracted.TotalAmount})
		}
	}
	// Generic loop for others if configured
	for k, v := range extracted.Entities {
		// Map "invoice_id" -> "Invoice Number"
		// This mapping logic should ideally be configurable or strict.
		// For now, let's map normalized keys to likely names.
		targetName := ""
		switch k {
		case "invoice_id":
			targetName = "Invoice Number"
		case "currency":
			targetName = "Currency"
		case "net_amount":
			targetName = "Net Amount"
		}

		if targetName != "" {
			if id, found := s.customFields[targetName]; found {
				cfs = append(cfs, paperless.CustomFieldInstance{Field: id, Value: v})
			}
		}
	}

	if len(cfs) > 0 {
		updates.CustomFields = cfs
	}

	if err := s.paperlessClient.UpdateDocument(docID, updates); err != nil {
		slog.Error("Update error", "document_id", docID, "error", err)
		return
	}

	slog.Info("Successfully processed and updated", "document_id", docID)
}

func (s *Server) getOrCreateCorrespondent(name string) (*paperless.Correspondent, error) {
	// 1. Try finding
	existing, err := s.paperlessClient.GetCorrespondent(name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}

	// 2. Create
	return s.paperlessClient.CreateCorrespondent(name)
}

func (s *Server) createLocalBill(docID int, extracted *docai.ExtractedData, doc *paperless.Document, req BillRequest, acClient *portal.Client) {
	slog.Info("Creating local portal bill", "document_id", docID, "supplier", extracted.Supplier)

	// Resolve vendor contact
	contactName := extracted.Supplier
	if contactName == "" {
		contactName = "Unknown Vendor"
	}

	contactID, err := acClient.GetOrCreateVendor(contactName)
	if err != nil {
		slog.Error("Portal contact error", "document_id", docID, "error", err)
		return
	}

	// Parse dates
	issuedAt := extracted.ExampleDate
	if issuedAt == "" {
		issuedAt = time.Now().Format("2006-01-02")
	}

	// Due date = issued + 30 days
	dueAt := issuedAt
	if t, err := time.Parse("2006-01-02", issuedAt); err == nil {
		dueAt = t.AddDate(0, 0, 30).Format("2006-01-02")
	}

	// Build amount (portal handles paise conversion)
	amount := decimalToAmount(extracted.TotalAmount)
	if amount <= 0 {
		slog.Warn("Skipping portal bill: no valid amount", "document_id", docID, "raw_amount", extracted.TotalAmount)
		return
	}

	// Invoice / document number
	docNumber := ""
	if val, ok := extracted.Entities["invoice_id"]; ok {
		docNumber = val
	}

	billInput := portal.BillInput{
		ContactID:  &contactID,
		BillNumber: docNumber,
		IssueDate:  issuedAt,
		DueDate:    dueAt,
		Amount:     amount,
		Status:     "draft",
		FileURL:    req.DocURL,
		Notes:      fmt.Sprintf("Auto-created from Paperless document #%d (%s)", docID, doc.OriginalFileName),
		Items:      buildBillLineItems(extracted.LineItems),
	}

	billID, err := acClient.CreateBill(billInput)
	if err != nil {
		slog.Error("Portal bill creation failed", "document_id", docID, "error", err)
		return
	}

	slog.Info("Local portal bill created", "document_id", docID, "portal_bill_id", billID)
}

func buildBillLineItems(extractedItems []docai.LineItem) []portal.BillLineItem {
	lineItems := make([]portal.BillLineItem, 0, len(extractedItems))

	for _, item := range extractedItems {
		lineItem := portal.BillLineItem{
			Description: strings.TrimSpace(item.Description),
			Quantity:    parseDecimal(item.Quantity),
			Unit:        strings.TrimSpace(item.Unit),
			UnitPrice:   decimalToAmount(item.UnitPrice),
			Amount:      decimalToAmount(item.Amount),
		}
		if lineItem.Amount == 0 && lineItem.Quantity > 0 && lineItem.UnitPrice > 0 {
			lineItem.Amount = roundToTwo(lineItem.Quantity * lineItem.UnitPrice)
		}
		if lineItem.Description == "" && lineItem.Quantity == 0 && lineItem.Unit == "" && lineItem.UnitPrice == 0 && lineItem.Amount == 0 {
			continue
		}
		if lineItem.Description == "" {
			slog.Warn("Skipping line item with empty description", "item", item)
			continue
		}
		if lineItem.Amount <= 0 {
			slog.Warn("Skipping line item with non-positive amount", "description", lineItem.Description, "amount", lineItem.Amount)
			continue
		}
		if lineItem.UnitPrice <= 0 {
			slog.Warn("Skipping line item with non-positive unit price", "description", lineItem.Description, "unit_price", lineItem.UnitPrice)
			continue
		}
		if lineItem.Quantity <= 0 {
			slog.Warn("Skipping line item with non-positive quantity", "description", lineItem.Description, "quantity", lineItem.Quantity)
			continue
		}
		lineItems = append(lineItems, lineItem)
	}

	return lineItems
}

func decimalToAmount(raw string) float64 {
	rat, ok := parseDecimalRat(raw)
	if !ok {
		return 0
	}

	// Convert to paise (integer) using half-up rounding to avoid binary float errors.
	paiseRat := new(big.Rat).Mul(rat, big.NewRat(100, 1))
	paiseInt := roundRatHalfUpToInt(paiseRat)

	return float64(paiseInt.Int64()) / 100
}

func roundToTwo(val float64) float64 {
	return math.Round(val*100) / 100
}

func parseDecimal(raw string) float64 {
	cleaned := strings.TrimSpace(raw)
	if cleaned == "" {
		return 0
	}

	// Strip currency symbols and other non-numeric characters, but keep
	// digits, decimal/thousands separators, and sign. Then treat commas
	// as thousands separators (remove them) before parsing.
	var b strings.Builder
	for _, r := range cleaned {
		if (r >= '0' && r <= '9') || r == '.' || r == ',' || r == '-' || r == '+' {
			b.WriteRune(r)
		}
	}
	normalized := strings.ReplaceAll(b.String(), ",", "")

	// Ensure we still have something that looks like a number.
	if normalized == "" || normalized == "-" || normalized == "+" || normalized == "." {
		slog.Warn("Failed to parse decimal: no numeric content after normalization", "raw", raw)
		return 0
	}

	value, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		slog.Warn("Failed to parse decimal", "raw", raw, "normalized", normalized, "error", err)
		return 0
	}
	return value
}

func parseDecimalRat(raw string) (*big.Rat, bool) {
	cleaned := strings.TrimSpace(raw)
	if cleaned == "" {
		return nil, false
	}

	var b strings.Builder
	for _, r := range cleaned {
		if (r >= '0' && r <= '9') || r == '.' || r == ',' || r == '-' || r == '+' {
			b.WriteRune(r)
		}
	}
	normalized := strings.ReplaceAll(b.String(), ",", "")

	if normalized == "" || normalized == "-" || normalized == "+" || normalized == "." {
		slog.Warn("Failed to parse decimal: no numeric content after normalization", "raw", raw)
		return nil, false
	}

	rat, ok := new(big.Rat).SetString(normalized)
	if !ok {
		slog.Warn("Failed to parse decimal to rational", "raw", raw, "normalized", normalized)
		return nil, false
	}
	return rat, true
}

func roundRatHalfUpToInt(rat *big.Rat) *big.Int {
	if rat == nil {
		return big.NewInt(0)
	}

	n := new(big.Int).Set(rat.Num())
	d := new(big.Int).Set(rat.Denom())

	q := new(big.Int).Quo(n, d)
	r := new(big.Int).Sub(n, new(big.Int).Mul(q, d))

	absR := new(big.Int).Abs(r)
	absD := new(big.Int).Abs(d)

	// Compare |r| * 2 with |d| for half-up rounding.
	doubleR := new(big.Int).Lsh(absR, 1)
	if doubleR.Cmp(absD) >= 0 {
		if n.Sign() >= 0 {
			q.Add(q, big.NewInt(1))
		} else {
			q.Sub(q, big.NewInt(1))
		}
	}
	return q
}

func (s *Server) handlePayouts(w http.ResponseWriter, r *http.Request) {
	if s.cfg.PortalURL == "" {
		http.Error(w, "Portal integration disabled", http.StatusServiceUnavailable)
		return
	}

	var req PayoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode payout request", "error", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	docID, err := extractDocIDFromURL(req.DocURL)
	if err != nil {
		slog.Error("Invalid document URL in payout request", "url", req.DocURL, "error", err)
		http.Error(w, "Invalid document URL", http.StatusBadRequest)
		return
	}

	slog.Info("Received payout request", "doc_url", req.DocURL, "document_id", docID)

	db, acClient, ok := s.openTenantResources(w, docID)
	if !ok {
		return
	}

	go s.processPayout(docID, req, db, acClient)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Payout processing started"))
}

func (s *Server) processPayout(docID int, req PayoutRequest, db *storage.DB, acClient *portal.Client) {
	defer db.Close()
	slog.Info("Starting payout processing", "document_id", docID)

	// 1. if the document already processed, return no need to process again
	if processed, err := db.IsDocumentProcessed(docID); err == nil && processed {
		slog.Warn("Document already processed, skipping it", "document_id", docID)
		return
	}

	// 1. Get Document (for tags)
	doc, err := s.paperlessClient.GetDocument(docID)
	if err != nil {
		slog.Error("Error getting payout document", "document_id", docID, "error", err)
		return
	}

	// 2. Get Metadata (for filename)
	meta, err := s.paperlessClient.GetMetadata(docID)
	if err != nil {
		slog.Error("Error getting payout metadata", "document_id", docID, "error", err)
		return
	}

	// 3. Determine DuckDB Options based on Tags
	var option config.PlatformConfig
	var platform string
	s.tagIDsMu.RLock()
	for name, id := range s.tagIDs {
		for _, tagID := range doc.Tags {
			if id == tagID {
				if cfg, ok := s.duckDBConfigs[tagID]; ok {
					option = cfg
					platform = name
					break
				}
			}
		}
		if platform != "" {
			break
		}
	}
	s.tagIDsMu.RUnlock()

	// Try to get file path from mounted media volume for DuckDB ProcessPlatformExcel
	filename := "documents/originals/" + meta.MediaFilename
	filePath := fmt.Sprintf("/app/media/%s", filename)

	if (strings.HasSuffix(strings.ToLower(filename), ".xlsx") || strings.HasSuffix(strings.ToLower(filename), ".xls")) && platform != "" {
		if option.UseLibreOffice() {
			if s.libreOfficeClient == nil || s.cfg == nil || s.cfg.LibreOfficeURL == "" {
				slog.Error("LibreOffice import method requested but LIBREOFFICE_URL is not configured", "document_id", docID)
				return
			}
			slog.Info("Excel file detected in payout, storing via LibreOffice parser", "path", filename, "platform", platform)

			// resultRowCounts tracks the number of data rows loaded for each
			// import config so that subsequent configs with RelativeRange can
			// compute their start row correctly.
			resultRowCounts := make([]int, len(option.ImportConfigs))

			for i, importConfig := range option.ImportConfigs {
				// Resolve relative range if configured.
				if importConfig.RelativeRange.RelativeConfigIndex > 0 {
					refIdx := importConfig.RelativeRange.RelativeConfigIndex
					if refIdx >= i {
						slog.Error("LibreOffice relative range: RelativeConfigIndex must reference an earlier config", "document_id", docID, "config_index", i, "relative_config_index", refIdx)
						return
					}
					relativeOption := option.ImportConfigs[refIdx]
					refRowCount := resultRowCounts[refIdx]
					// Mirror the DuckDB GetRangeEnd row-count adjustment:
					// decrement only when the referenced config explicitly sets
					// header=false.
					if relativeOption.Header != nil && !*relativeOption.Header {
						refRowCount--
					}
					// If the referenced config had a footer row, that row is
					// physically present in the spreadsheet even though it was
					// stripped from the loaded data.  Add it back so the computed
					// start row accounts for the full physical extent of the table.
					if relativeOption.Footer != nil && *relativeOption.Footer {
						refRowCount++
					}
					if refRowCount < 0 {
						refRowCount = 0
					}
					refRange, err := excel.NewRange(relativeOption.Range)
					if err != nil {
						slog.Error("LibreOffice relative range: failed to parse referenced range", "document_id", docID, "range", relativeOption.Range, "error", err)
						return
					}
					// Build the new range using the referenced config's columns
					// but with a computed start row.
					refRange.Start.Row = refRange.Start.Row + refRowCount + importConfig.RelativeRange.RowsOffset
					importConfig.Range = refRange.String()
					slog.Debug("LibreOffice relative range resolved", "config_index", i, "computed_range", importConfig.Range)
				}

				hasHeader := importConfig.Header == nil || *importConfig.Header
				stopAtEmpty := importConfig.StopAtEmpty != nil && *importConfig.StopAtEmpty
				result, err := s.libreOfficeClient.Parse(filename, importConfig.Sheet, importConfig.Range, hasHeader, stopAtEmpty)
				if err != nil {
					slog.Error("LibreOffice parse failed", "document_id", docID, "sheet", importConfig.Sheet, "error", err)
					return
				}

				// Drop the last row when the import config declares a footer
				// row (i.e. a totals/summary row appended by Excel).
				if importConfig.Footer != nil && *importConfig.Footer && len(result.Rows) > 0 {
					slog.Debug("LibreOffice footer row dropped", "table", importConfig.GetTableName(platform), "rows_before", len(result.Rows))
					result.Rows = result.Rows[:len(result.Rows)-1]
				}

				resultRowCounts[i] = len(result.Rows)

				tableName := importConfig.GetTableName(platform)
				if err := db.LoadRowsIntoTable(docID, tableName, result); err != nil {
					slog.Error("Failed to load LibreOffice rows into table", "document_id", docID, "table", tableName, "error", err)
					return
				}
			}
		} else {
			slog.Info("Excel file detected in payout, storing via Nexus gateway", "path", filePath, "platform", platform, "options", option)

			if err := db.ProcessPlatformExcel(docID, filePath, platform, option); err != nil {
				slog.Error("Nexus gateway ProcessPlatformExcel failed", "document_id", docID, "error", err)
				return
			}
		}

		payoutInput, err := db.GetPlatformExcelRows(docID, platform, option)
		if err != nil {
			slog.Error("Failed to get excel rows", "document_id", docID, "error", err)
			return
		}

		payoutInput.Platform = portal.Platform(platform)
		payoutInput.OutletName = "Noodle House"

		// swiggy sends the amount as negative, so adding it
		payoutInput.FinalPayoutAmt += payoutInput.MarketingAdsAmt

		slog.Debug("Extracted payout data from DB", "document_id", docID, "payout_input", payoutInput.String())

		// 5. Send to Portal
		payoutID, err := acClient.CreatePayout(payoutInput)
		if err != nil {
			slog.Error("Portal payout creation failed", "document_id", docID, "error", err)
			return
		}

		// 6. Save to processed documents
		doc := storage.ProcessedDocument{
			PaperlessID: docID,
			Filename:    filename,
		}
		err = db.SaveDocument(&doc)

		slog.Info("Local portal payout created from Excel", "document_id", docID, "payout_id", payoutID)
	} else {
		// Payout with generic document (TIKA or DocAI)
		// ... existing implementation if any ...
	}
}

func (s *Server) handleBankStatements(w http.ResponseWriter, r *http.Request) {
	if s.cfg.PortalURL == "" {
		http.Error(w, "Portal integration disabled", http.StatusServiceUnavailable)
		return
	}

	var req BankStatementRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode bank statement request", "error", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	docID, err := extractDocIDFromURL(req.DocURL)
	if err != nil {
		slog.Error("Invalid document URL in bank statement request", "url", req.DocURL, "error", err)
		http.Error(w, "Invalid document URL", http.StatusBadRequest)
		return
	}

	slog.Info("Received bank statement request", "doc_url", req.DocURL, "document_id", docID)

	db, acClient, ok := s.openTenantResources(w, docID)
	if !ok {
		return
	}

	go s.processBankStatement(docID, req, db, acClient)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Bank statement processing started"))
}

func (s *Server) processBankStatement(docID int, req BankStatementRequest, db *storage.DB, acClient *portal.Client) {
	defer db.Close()
	slog.Info("Starting bank statement processing", "document_id", docID)

	// 1. Get Metadata & Content
	content, err := s.paperlessClient.DownloadDocument(docID, false)
	if err != nil {
		slog.Error("Error downloading bank statement", "document_id", docID, "error", err)
		return
	}

	mtype := mimetype.Detect(content)
	mimeType := mtype.String()

	// 3. Process with DocAI (using BankStatementProcessorID)
	aiDoc, err := s.docAIClient.ProcessDocument(context.Background(), s.cfg.BankStatementProcessorID, content, mimeType)
	if err != nil {
		slog.Error("DocAI bank statement error", "document_id", docID, "error", err)
		return
	}

	// 3a. Save to processed documents
	// 4. Save to DB
	// Serialize Extracted + Full Response?
	// We'll just save the extracted for now + raw JSON if we had it (aiDoc is protobuf)
	// For "raw_ocr_data", we can marshal aiDoc to JSON.
	rawJSON, _ := json.Marshal(aiDoc.Entities)

	// totalAmount, _ := strconv.ParseFloat(extracted.TotalAmount, 64) // weak parsing, clean up usually needed (remove currency symbols)

	doc := &storage.ProcessedDocument{
		PaperlessID: docID,
		Filename:    req.DocURL,
		// Supplier:      extracted.Supplier,
		// Date:          extracted.ExampleDate,
		// TotalAmount:   totalAmount,
		RawOCRData:    string(rawJSON),
		ExtractedText: aiDoc.Text,
	}

	err = db.SaveDocument(doc)
	if err != nil {
		slog.Error("Failed to save document", "document_id", docID, "error", err)
		return
	}

	// 4. Extract Transactions
	transactions := s.docAIClient.ExtractBankStatementData(aiDoc)
	slog.Info("Extracted transactions", "document_id", docID, "count", len(transactions))

	// 5. Send to Portal
	if acClient != nil && len(transactions) > 0 {
		// Resolve bank name from DocAI top-level entities (type = "bank_name")
		bankName := "Bank"
		for _, entity := range aiDoc.Entities {
			if entity.Type == "bank_name" {
				v := entity.MentionText
				if entity.NormalizedValue != nil && entity.NormalizedValue.Text != "" {
					v = entity.NormalizedValue.Text
				}
				if v != "" {
					bankName = v
					break
				}
			}
		}
		slog.Info("Resolved bank name from DocAI", "bank_name", bankName)

		bankAccountID, err := acClient.GetOrCreateBankAccount(bankName)
		if err != nil {
			slog.Error("Failed to get/create bank account", "document_id", docID, "bank_name", bankName, "error", err)
			// Continue without portal — don't abort
		} else {
			for _, txMap := range transactions {
				amount, _ := strconv.ParseFloat(txMap["amount"], 64)

				// Map debit → expense, credit → income (portal service expects income/expense)
				txType := "expense"
				if txMap["type"] == "credit" {
					txType = "income"
				}

				date := txMap["date"]
				if date == "" {
					date = time.Now().Format("2006-01-02")
				}
				desc := txMap["description"]

				txnInput := portal.TransactionInput{
					AccountID:       bankAccountID,
					Type:            txType,
					Amount:          amount,
					TransactionDate: &date,
					Description:     &desc,
				}

				txID, err := acClient.CreateTransaction(txnInput)
				if err != nil {
					slog.Error("Failed to create transaction", "document_id", docID, "error", err, "date", date, "amount", amount, "type", txType)
					continue
				}
				slog.Info("Transaction created", "document_id", docID, "transaction_id", txID, "type", txType, "amount", amount)
			}
		}
	}

	// 6. Update Paperless
	updates := paperless.DocumentUpdate{
		Content: &aiDoc.Text,
	}
	if err := s.paperlessClient.UpdateDocument(docID, updates); err != nil {
		slog.Warn("Failed to update paperless document content", "document_id", docID, "error", err)
	}

	slog.Info("Finished processing bank statement", "document_id", docID)
}

func (s *Server) parseAmount(val string) int {
	// Clean currency symbols, commas, and Tika wrappers like [$₹]
	val = strings.ReplaceAll(val, "₹", "")
	val = strings.ReplaceAll(val, "[$₹]", "")
	val = strings.ReplaceAll(val, ",", "")
	val = strings.TrimSpace(val)
	amtFloat, _ := strconv.ParseFloat(val, 64)
	return int(amtFloat * 100)
}
