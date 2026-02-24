package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"paperless-document-processor/config"
	"paperless-document-processor/pkg/accounting"
	"paperless-document-processor/pkg/docai"
	"paperless-document-processor/pkg/paperless"
	"paperless-document-processor/pkg/storage"
	"paperless-document-processor/pkg/tika"

	"github.com/gabriel-vasile/mimetype"
)

type Server struct {
	cfg              *config.Config
	db               *storage.DB
	paperlessClient  *paperless.Client
	docAIClient      *docai.Client
	accountingClient *accounting.Client // nil if not configured
	tikaClient       *tika.Client       // nil if not configured
	customFields     map[string]int     // Name -> ID
	tagIDs           map[string]int     // Name -> ID (e.g., "Swiggy" -> 3)
	duckDBConfigs    map[int]config.PlatformConfig
}

type BillRequest struct {
	DocURL string `json:"doc_url"`
}

type PayoutRequest struct {
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

	// 3. Init DB
	db, err := storage.InitDB(cfg.DBPath)
	if err != nil {
		slog.Error("Failed to init db", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// 3. Init Clients
	pClient := paperless.NewClient(cfg.PaperlessURL, cfg.PaperlessToken)

	ctx := context.Background()
	dClient, err := docai.NewClient(ctx, cfg.GoogleProjectID, cfg.GoogleLocation, cfg.DocumentAIProcessorID, cfg.GoogleCredentialsPath)
	if err != nil {
		slog.Error("Failed to init DocAI client", "error", err)
		os.Exit(1)
	}
	defer dClient.Close()

	// Init Accounting client (optional)
	var acClient *accounting.Client
	if cfg.AccountingURL != "" {
		acClient = accounting.NewClient(cfg.AccountingURL, cfg.AccountingUser, cfg.AccountingPass)
		slog.Info("Accounting integration enabled", "url", cfg.AccountingURL)
	} else {
		slog.Info("Accounting integration disabled (ACCOUNTING_URL not set)")
	}

	srv := &Server{
		cfg:              cfg,
		db:               db,
		paperlessClient:  pClient,
		docAIClient:      dClient,
		accountingClient: acClient,
		tikaClient:       tika.NewClient(cfg.TikaURL),
		customFields:     make(map[string]int),
		tagIDs:           make(map[string]int),
		duckDBConfigs:    make(map[int]config.PlatformConfig),
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
	slog.Info("Starting server", "port", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, nil); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}

func (s *Server) handleBills(w http.ResponseWriter, r *http.Request) {

	var req BillRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode bill request", "error", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Extract ID from URL (e.g. http://webserver:8000/documents/73/)
	trimmed := strings.TrimSuffix(req.DocURL, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		http.Error(w, "Invalid doc_url format", http.StatusBadRequest)
		return
	}
	idStr := parts[len(parts)-1]

	docID, err := strconv.Atoi(idStr)
	if err != nil {
		slog.Error("Invalid document ID format from url", "url", req.DocURL, "id_part", idStr, "error", err)
		http.Error(w, "Invalid document ID in URL", http.StatusBadRequest)
		return
	}

	slog.Info("Received bill request", "doc_url", req.DocURL, "document_id", docID)

	// Run processing asynchronously
	go s.processBill(docID, req)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Processing started"))
}

func (s *Server) processBill(docID int, req BillRequest) {
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
	aiDoc, err := s.docAIClient.ProcessDocument(context.Background(), content, mimeType)
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

	if err := s.db.SaveDocument(dbDoc); err != nil {
		slog.Error("DB Save error", "document_id", docID, "error", err)
		// Continue anyway? Yes.
	}

	// 4b. Create Bill in Accounting (optional)
	if s.accountingClient != nil {
		s.createLocalBill(docID, extracted, doc, req)
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

func (s *Server) createLocalBill(docID int, extracted *docai.ExtractedData, doc *paperless.Document, req BillRequest) {
	slog.Info("Creating local accounting bill", "document_id", docID, "supplier", extracted.Supplier)

	// Resolve vendor contact
	contactName := extracted.Supplier
	if contactName == "" {
		contactName = "Unknown Vendor"
	}

	contactID, err := s.accountingClient.GetOrCreateVendor(contactName)
	if err != nil {
		slog.Error("Accounting contact error", "document_id", docID, "error", err)
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

	// Build amount in paise
	amountFloat, _ := strconv.ParseFloat(extracted.TotalAmount, 64)
	amountPaise := int(amountFloat * 100)
	if amountPaise <= 0 {
		slog.Warn("Skipping accounting bill: no valid amount", "document_id", docID, "raw_amount", extracted.TotalAmount)
		return
	}

	// Invoice / document number
	docNumber := ""
	if val, ok := extracted.Entities["invoice_id"]; ok {
		docNumber = val
	}

	billInput := accounting.BillInput{
		ContactID:  &contactID,
		BillNumber: docNumber,
		IssueDate:  issuedAt,
		DueDate:    dueAt,
		Amount:     amountPaise,
		Status:     "draft",
		FileURL:    req.DocURL,
		Notes:      fmt.Sprintf("Auto-created from Paperless document #%d (%s)", docID, doc.OriginalFileName),
	}

	billID, err := s.accountingClient.CreateBill(billInput)
	if err != nil {
		slog.Error("Accounting bill creation failed", "document_id", docID, "error", err)
		return
	}

	slog.Info("Local accounting bill created", "document_id", docID, "accounting_bill_id", billID)
}

func (s *Server) handlePayouts(w http.ResponseWriter, r *http.Request) {
	if s.accountingClient == nil {
		http.Error(w, "Accounting integration disabled", http.StatusServiceUnavailable)
		return
	}

	var req PayoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode payout request", "error", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Extract ID from URL
	trimmed := strings.TrimSuffix(req.DocURL, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		http.Error(w, "Invalid doc_url format", http.StatusBadRequest)
		return
	}
	idStr := parts[len(parts)-1]
	docID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid document ID in URL", http.StatusBadRequest)
		return
	}

	slog.Info("Received payout request", "doc_url", req.DocURL, "document_id", docID)

	go s.processPayout(docID, req)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Payout processing started"))
}

func (s *Server) processPayout(docID int, req PayoutRequest) {
	slog.Info("Starting payout processing", "document_id", docID)

	// 1. if the document already processed, return no need to process again
	if processed, err := s.db.IsDocumentProcessed(docID); err == nil && processed {
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

	// Try to get file path from mounted media volume for DuckDB ProcessPlatformExcel
	filename := "documents/originals/" + meta.MediaFilename
	filePath := fmt.Sprintf("/app/media/%s", filename)

	if (strings.HasSuffix(strings.ToLower(filename), ".xlsx") || strings.HasSuffix(strings.ToLower(filename), ".xls")) && platform != "" {
		slog.Info("Excel file detected in payout, storing via DuckDB", "path", filePath, "platform", platform, "options", option)

		if err := s.db.ProcessPlatformExcel(docID, filePath, platform, option); err != nil {
			slog.Error("DuckDB ProcessPlatformExcel failed", "document_id", docID, "error", err)
			return
		}

		payoutInput, err := s.db.GetPlatformExcelRows(docID, platform, option)
		if err != nil {
			slog.Error("Failed to get excel rows", "document_id", docID, "error", err)
			return
		}

		payoutInput.Platform = accounting.Platform(platform)
		payoutInput.OutletName = "Noodle House"

		// swiggy sends the amount as negative, so adding it
		payoutInput.FinalPayoutAmt += payoutInput.MarketingAdsAmt

		if payoutInput.UtrNumber == "" {
			payoutInput.UtrNumber = doc.OriginalFileName // Fallback
		}
		slog.Debug("Extracted payout data from DB", "document_id", docID, "payout_input", payoutInput.String())

		// 5. Send to Accounting
		payoutID, err := s.accountingClient.CreatePayout(payoutInput)
		if err != nil {
			slog.Error("Accounting payout creation failed", "document_id", docID, "error", err)
			return
		}

		// 6. Save to processed documents
		doc := storage.ProcessedDocument{
			PaperlessID: docID,
			Filename:    filename,
		}
		err = s.db.SaveDocument(&doc)

		slog.Info("Local accounting payout created from Excel", "document_id", docID, "payout_id", payoutID)
	} else {
		// 2. Download Content
		// content, err := s.paperlessClient.DownloadDocument(docID, true)
		// if err != nil {
		// 	slog.Error("Error downloading payout content", "document_id", docID, "error", err)
		// 	return
		// }

		// 3. Process with Tika
		// slog.Info("Sending to Tika for parsing", "document_id", docID)
		// text, err := s.tikaClient.Parse(content)
		// if err != nil {
		// 	slog.Error("Tika error", "document_id", docID, "error", err)
		// 	return
		// }

		// // 4. Extract Data
		// // payoutInput := s.extractPayoutData(text)
		// if payoutInput.Platform == "" && platform != "" {
		// 	payoutInput.Platform = accounting.Platform(platform)
		// }
		// if payoutInput.UtrNumber == "" {
		// 	payoutInput.UtrNumber = doc.OriginalFileName // Fallback
		// }
		// slog.Debug("Extracted payout data via Tika", "document_id", docID, "payout_input", payoutInput.String())

		// // 5. Send to Accounting
		// payoutID, err := s.accountingClient.CreatePayout(payoutInput)
		// if err != nil {
		// 	slog.Error("Accounting payout creation failed via Tika", "document_id", docID, "error", err)
		// 	return
		// }

		// slog.Info("Local accounting payout created via Tika", "document_id", docID, "payout_id", payoutID)
	}
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
