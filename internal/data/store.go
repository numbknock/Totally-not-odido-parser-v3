package data

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

const indexVersion = "8"

const (
	defaultCommitBatchRows      int64 = 50000
	defaultProgressEvery        int64 = 50000
	defaultJSONFieldInsertBatch       = 2048
)

type Store struct {
	db          *sql.DB
	datasetPath string
	datasetFile *os.File
	fastIndex   bool
	ftsBroad    bool
	statusMu    sync.RWMutex
	status      IndexStatus
}

type Record struct {
	RowNum         int64  `json:"row_num"`
	ID             string `json:"id"`
	Name           string `json:"name"`
	Phone          string `json:"phone"`
	HasSObjectLog  bool   `json:"has_sobject_log"`
	HasFlashMsg    bool   `json:"has_flash_message"`
	Type           string `json:"type"`
	Status         string `json:"status"`
	Segment        string `json:"segment"`
	SalesChannel   string `json:"sales_channel"`
	BillingCity    string `json:"billing_city"`
	BillingState   string `json:"billing_state"`
	BillingCountry string `json:"billing_country"`
	PostalCode     string `json:"postal_code"`
	CreatedDate    string `json:"created_date"`
	ModifiedDate   string `json:"modified_date"`
	IsActive       bool   `json:"is_active"`
	IsDeleted      bool   `json:"is_deleted"`
}

type QueryParams struct {
	Q            string
	Type         string
	Status       string
	Country      string
	City         string
	Active       string
	ModifiedFrom string
	ModifiedTo   string
	JSONPath     string
	JSONValue    string
	JSONOp       string
	Sort         string
	UniqueIDs    bool
	Limit        int
	Offset       int
}

type QueryResult struct {
	Records []Record `json:"records"`
	Total   int64    `json:"total"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
}

type CountBucket struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type Stats struct {
	TotalRows       int64         `json:"total_rows"`
	ActiveRows      int64         `json:"active_rows"`
	InactiveRows    int64         `json:"inactive_rows"`
	DeletedRows     int64         `json:"deleted_rows"`
	FirstModifiedAt string        `json:"first_modified_at"`
	LastModifiedAt  string        `json:"last_modified_at"`
	TopTypes        []CountBucket `json:"top_types"`
	TopStatuses     []CountBucket `json:"top_statuses"`
	TopCountries    []CountBucket `json:"top_countries"`
	TopCities       []CountBucket `json:"top_cities"`
}

type Facets struct {
	Types     []string `json:"types"`
	Statuses  []string `json:"statuses"`
	Countries []string `json:"countries"`
	Cities    []string `json:"cities"`
}

type Health struct {
	Ready       bool   `json:"ready"`
	DatasetPath string `json:"dataset_path"`
	DBPath      string `json:"db_path"`
	Rows        int64  `json:"rows"`
	IndexedAt   string `json:"indexed_at"`
	Indexing    bool   `json:"indexing"`
	Step        string `json:"step"`
	Message     string `json:"message,omitempty"`
	RowsIndexed int64  `json:"rows_indexed,omitempty"`
	ParseErrors int64  `json:"parse_errors,omitempty"`
}

type IndexStatus struct {
	Ready       bool   `json:"ready"`
	Indexing    bool   `json:"indexing"`
	Step        string `json:"step"`
	Message     string `json:"message,omitempty"`
	RowsIndexed int64  `json:"rows_indexed,omitempty"`
	ParseErrors int64  `json:"parse_errors,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	IndexedAt   string `json:"indexed_at,omitempty"`
}

type AnalyticsField struct {
	Name  string `json:"name"`
	Label string `json:"label"`
}

type AnalyticsBucket struct {
	Value      string  `json:"value"`
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
}

type AnalyticsDistribution struct {
	Field         string            `json:"field"`
	Filter        string            `json:"filter"`
	Limit         int               `json:"limit"`
	TotalRows     int64             `json:"total_rows"`
	MatchedRows   int64             `json:"matched_rows"`
	DistinctCount int64             `json:"distinct_count"`
	Buckets       []AnalyticsBucket `json:"buckets"`
}

type AnalyticsCountResult struct {
	Field string `json:"field"`
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type analyticsFieldSpec struct {
	Name  string
	Label string
	Expr  string
}

type jsonIndexedField struct {
	Path      string
	ValueText string
	ValueType string
}

var analyticsLabelOverrides = map[string]string{
	"id":           "ID",
	"is_active":    "Active Flag",
	"is_deleted":   "Deleted Flag",
	"email_domain": "Email Domain",
}

type sourceRecord struct {
	ID           string `json:"Id"`
	Name         string `json:"Name"`
	Email        string `json:"Email"`
	Phone        string `json:"Phone"`
	MobilePhone  string `json:"MobilePhone"`
	HomePhone    string `json:"HomePhone"`
	OtherPhone   string `json:"OtherPhone"`
	PersonPhone  string `json:"PersonPhone"`
	PersonMobile string `json:"PersonMobilePhone"`
	PersonHome   string `json:"PersonHomePhone"`
	PersonOther  string `json:"PersonOtherPhone"`
	SObjectLog   string `json:"SObjectLog__c"`
	FlashMessage string `json:"Flash_Message__c"`
	SearchBlob   string
	Type         string `json:"Type"`
	AttrType     string `json:"attributes_type"`
	Status       string `json:"vlocity_cmt__Status__c"`
	Segment      string `json:"Segment__c"`
	SalesChannel string `json:"Sales_Channel__c"`

	BillingCity    string `json:"BillingCity"`
	BillingState   string `json:"BillingState"`
	BillingCountry string `json:"BillingCountry"`
	CountryCode    string `json:"CountryCode__c"`
	PostalCode     string `json:"BillingPostalCode"`

	CreatedDate  string `json:"CreatedDate"`
	ModifiedDate string `json:"LastModifiedDate"`
	IsActive     string `json:"IsActive"`
	IsDeleted    string `json:"IsDeleted"`
}

type indexConfig struct {
	ParseWorkers         int
	CommitBatchRows      int64
	ProgressEvery        int64
	JSONFieldInsertBatch int
	IndexJSONFields      bool
	FTSBroad             bool
}

func NewStore(datasetPath, dbConnStr string, fastIndex, ftsBroad bool) (*Store, error) {
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		return nil, err
	}
	// PostgreSQL can handle much higher concurrent connections than SQLite
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	file, err := os.Open(datasetPath)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:          db,
		datasetPath: datasetPath,
		datasetFile: file,
		fastIndex:   fastIndex,
		ftsBroad:    ftsBroad,
		status: IndexStatus{
			Ready:    false,
			Indexing: false,
			Step:     "startup",
		},
	}, nil
}

func (s *Store) Close() error {
	var errs []error
	if s.datasetFile != nil {
		errs = append(errs, s.datasetFile.Close())
	}
	if s.db != nil {
		errs = append(errs, s.db.Close())
	}
	return errors.Join(errs...)
}

func (s *Store) EnsureIndex(ctx context.Context) error {
	now := time.Now().UTC().Format(time.RFC3339)
	s.updateStatus(func(st *IndexStatus) {
		st.Indexing = true
		st.Ready = false
		st.Step = "checking"
		st.Message = "checking index state"
		st.StartedAt = now
		st.UpdatedAt = now
		st.RowsIndexed = 0
		st.ParseErrors = 0
	})

	ok, err := s.isIndexCurrent(ctx)
	if err != nil {
		s.updateStatus(func(st *IndexStatus) {
			st.Indexing = false
			st.Ready = false
			st.Step = "error"
			st.Message = err.Error()
			st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		})
		return err
	}
	if ok {
		indexedAt, _ := s.metaValue(ctx, "indexed_at")
		s.updateStatus(func(st *IndexStatus) {
			st.Indexing = false
			st.Ready = true
			st.Step = "ready"
			st.Message = "index is current"
			st.IndexedAt = indexedAt
			st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		})
		return nil
	}

	s.updateStatus(func(st *IndexStatus) {
		st.Step = "rebuilding"
		st.Message = "rebuilding index"
		st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	})
	if err := s.rebuildIndex(ctx); err != nil {
		s.updateStatus(func(st *IndexStatus) {
			st.Indexing = false
			st.Ready = false
			st.Step = "error"
			st.Message = err.Error()
			st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		})
		return err
	}
	indexedAt, _ := s.metaValue(ctx, "indexed_at")
	s.updateStatus(func(st *IndexStatus) {
		st.Indexing = false
		st.Ready = true
		st.Step = "ready"
		st.Message = "index ready"
		st.IndexedAt = indexedAt
		st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	})
	return nil
}

func (s *Store) EnsureTables(ctx context.Context) error {
	schemaSQL := `
		CREATE TABLE IF NOT EXISTS records (
			row_num BIGSERIAL PRIMARY KEY,
			id TEXT,
			name TEXT,
			email TEXT,
			phone TEXT,
			has_sobject_log INTEGER NOT NULL,
			has_flash_message INTEGER NOT NULL,
			search_blob TEXT,
			type TEXT,
			status TEXT,
			segment TEXT,
			sales_channel TEXT,
			billing_city TEXT,
			billing_state TEXT,
			billing_country TEXT,
			postal_code TEXT,
			created_date TEXT,
			modified_date TEXT,
			is_active INTEGER NOT NULL,
			is_deleted INTEGER NOT NULL,
			file_offset BIGINT NOT NULL,
			line_length BIGINT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS record_json_fields (
			row_num BIGINT NOT NULL,
			path TEXT NOT NULL,
			value_text TEXT NOT NULL,
			value_type TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_records_id ON records(id);
		CREATE INDEX IF NOT EXISTS idx_records_phone ON records(phone);
		CREATE INDEX IF NOT EXISTS idx_records_haslog ON records(has_sobject_log);
		CREATE INDEX IF NOT EXISTS idx_records_hasflash ON records(has_flash_message);
		CREATE INDEX IF NOT EXISTS idx_records_type ON records(type);
		CREATE INDEX IF NOT EXISTS idx_records_status ON records(status);
		CREATE INDEX IF NOT EXISTS idx_records_country ON records(billing_country);
		CREATE INDEX IF NOT EXISTS idx_records_city ON records(billing_city);
		CREATE INDEX IF NOT EXISTS idx_records_modified ON records(modified_date);
		CREATE INDEX IF NOT EXISTS idx_records_active ON records(is_active);
		CREATE INDEX IF NOT EXISTS idx_json_fields_path_value ON record_json_fields(path, value_text);
		CREATE INDEX IF NOT EXISTS idx_json_fields_value ON record_json_fields(value_text);
		CREATE INDEX IF NOT EXISTS idx_json_fields_row ON record_json_fields(row_num);
	`
	_, err := s.db.ExecContext(ctx, schemaSQL)
	return err
}

func (s *Store) Health(ctx context.Context) (Health, error) {
	var rows int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM records`).Scan(&rows); err != nil {
		if !isNoSuchTableErr(err) {
			return Health{}, err
		}
		rows = 0
	}

	indexedAt, _ := s.metaValue(ctx, "indexed_at")
	// PostgreSQL doesn't use file paths like SQLite; return connection info
	dbPath := "postgresql://localhost/dataset"
	st := s.IndexStatus()

	return Health{
		Ready:       st.Ready,
		DatasetPath: s.datasetPath,
		DBPath:      dbPath,
		Rows:        rows,
		IndexedAt:   indexedAt,
		Indexing:    st.Indexing,
		Step:        st.Step,
		Message:     st.Message,
		RowsIndexed: st.RowsIndexed,
		ParseErrors: st.ParseErrors,
	}, nil
}

func (s *Store) IndexStatus() IndexStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

func (s *Store) updateStatus(fn func(*IndexStatus)) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	fn(&s.status)
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var out Stats
	if total, err := s.metaValue(ctx, "cached_total_rows"); err == nil {
		if active, err2 := s.metaValue(ctx, "cached_active_rows"); err2 == nil {
			if inactive, err3 := s.metaValue(ctx, "cached_inactive_rows"); err3 == nil {
				out.TotalRows, _ = strconv.ParseInt(total, 10, 64)
				out.ActiveRows, _ = strconv.ParseInt(active, 10, 64)
				out.InactiveRows, _ = strconv.ParseInt(inactive, 10, 64)
				out.DeletedRows = 0
				out.LastModifiedAt, _ = s.metaValue(ctx, "cached_last_modified_at")
				return out, nil
			}
		}
	}

	if err := s.queryRowContext(ctx, `SELECT COUNT(1) FROM records`).Scan(&out.TotalRows); err != nil {
		return Stats{}, err
	}
	if err := s.queryRowContext(ctx, `SELECT COUNT(1) FROM records WHERE is_active = 1`).Scan(&out.ActiveRows); err != nil {
		return Stats{}, err
	}
	if err := s.queryRowContext(ctx, `SELECT COUNT(1) FROM records WHERE is_active = 0`).Scan(&out.InactiveRows); err != nil {
		return Stats{}, err
	}
	if err := s.queryRowContext(ctx, `SELECT COUNT(1) FROM records WHERE is_deleted = 1`).Scan(&out.DeletedRows); err != nil {
		return Stats{}, err
	}
	if err := s.queryRowContext(ctx, `SELECT COALESCE(MAX(NULLIF(modified_date, '')), '') FROM records`).Scan(&out.LastModifiedAt); err != nil {
		return Stats{}, err
	}
	// Cache lightweight stats for faster UI health during heavy background indexing.
	_, _ = s.execContext(ctx, `
		INSERT INTO metadata(key, value) VALUES('cached_total_rows', ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(out.TotalRows, 10))
	_, _ = s.execContext(ctx, `
		INSERT INTO metadata(key, value) VALUES('cached_active_rows', ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(out.ActiveRows, 10))
	_, _ = s.execContext(ctx, `
		INSERT INTO metadata(key, value) VALUES('cached_inactive_rows', ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(out.InactiveRows, 10))
	_, _ = s.execContext(ctx, `
		INSERT INTO metadata(key, value) VALUES('cached_last_modified_at', ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, out.LastModifiedAt)

	return out, nil
}

func (s *Store) Facets(ctx context.Context, limit int) (Facets, error) {
	if limit <= 0 {
		limit = 100
	}

	types, err := s.distinctValues(ctx, "type", limit)
	if err != nil {
		return Facets{}, err
	}
	statuses, err := s.distinctValues(ctx, "status", limit)
	if err != nil {
		return Facets{}, err
	}
	countries, err := s.distinctValues(ctx, "billing_country", limit)
	if err != nil {
		return Facets{}, err
	}
	cities, err := s.distinctValues(ctx, "billing_city", limit)
	if err != nil {
		return Facets{}, err
	}

	return Facets{
		Types:     types,
		Statuses:  statuses,
		Countries: countries,
		Cities:    cities,
	}, nil
}

func (s *Store) QueryRecords(ctx context.Context, params QueryParams) (QueryResult, error) {
	if params.Limit <= 0 {
		params.Limit = 50
	}
	if params.Limit > 200 {
		params.Limit = 200
	}
	if params.Offset < 0 {
		params.Offset = 0
	}

	whereParts := []string{"1=1"}
	args := make([]any, 0, 8)

	if params.Type != "" {
		whereParts = append(whereParts, "r.type = ?")
		args = append(args, params.Type)
	}
	if params.Status != "" {
		whereParts = append(whereParts, "r.status = ?")
		args = append(args, params.Status)
	}
	if params.Country != "" {
		whereParts = append(whereParts, "r.billing_country = ?")
		args = append(args, params.Country)
	}
	if params.City != "" {
		whereParts = append(whereParts, "r.billing_city = ?")
		args = append(args, params.City)
	}
	if params.Active == "true" {
		whereParts = append(whereParts, "r.is_active = 1")
	}
	if params.Active == "false" {
		whereParts = append(whereParts, "r.is_active = 0")
	}
	if params.ModifiedFrom != "" {
		whereParts = append(whereParts, "r.modified_date >= ?")
		args = append(args, params.ModifiedFrom)
	}
	if params.ModifiedTo != "" {
		whereParts = append(whereParts, "r.modified_date <= ?")
		args = append(args, params.ModifiedTo)
	}
	if params.JSONPath != "" || params.JSONValue != "" {
		jsonParts := []string{"jf.row_num = r.row_num"}
		if params.JSONPath != "" {
			jsonParts = append(jsonParts, "jf.path = ?")
			args = append(args, params.JSONPath)
		}
		if params.JSONValue != "" {
			switch params.JSONOp {
			case "", "eq":
				jsonParts = append(jsonParts, "jf.value_text = ?")
				args = append(args, params.JSONValue)
			case "contains":
				jsonParts = append(jsonParts, "jf.value_text LIKE ?")
				args = append(args, "%"+params.JSONValue+"%")
			default:
				return QueryResult{}, fmt.Errorf("unsupported json_op %q", params.JSONOp)
			}
		}
		whereParts = append(whereParts, "EXISTS (SELECT 1 FROM record_json_fields jf WHERE "+strings.Join(jsonParts, " AND ")+")")
	}

	queryText := strings.TrimSpace(params.Q)
	if queryText != "" {
		// PostgreSQL: Use ILIKE for case-insensitive searches instead of FTS
		needle := "%" + queryText + "%"
		whereParts = append(whereParts, `(
			COALESCE(r.id, '') ILIKE ? OR
			COALESCE(r.name, '') ILIKE ? OR
			COALESCE(r.email, '') ILIKE ? OR
			COALESCE(r.search_blob, '') ILIKE ?
		)`)
		args = append(args, needle, needle, needle, needle)
	}

	whereSQL := strings.Join(whereParts, " AND ")

	orderBy := "r.modified_date DESC, r.row_num DESC"
	switch params.Sort {
	case "modified_asc":
		orderBy = "r.modified_date ASC, r.row_num ASC"
	case "name_asc":
		orderBy = "r.name ASC, r.row_num ASC"
	case "name_desc":
		orderBy = "r.name DESC, r.row_num DESC"
	case "city_asc":
		orderBy = "r.billing_city ASC, r.row_num ASC"
	case "city_desc":
		orderBy = "r.billing_city DESC, r.row_num DESC"
	}

	var countSQL string
	var querySQL string
	if params.UniqueIDs {
		countSQL = `
			SELECT COUNT(1)
			FROM (
				SELECT MAX(r.row_num)
				FROM records r
				WHERE ` + whereSQL + `
				GROUP BY CASE
					WHEN COALESCE(r.id, '') <> '' THEN r.id
					ELSE printf('row:%d', r.row_num)
				END
			) d`
		querySQL = `
			SELECT r.row_num, r.id, r.name, COALESCE(r.phone, ''), COALESCE(r.has_sobject_log, 0), COALESCE(r.has_flash_message, 0), r.type, r.status, r.segment, r.sales_channel,
			       r.billing_city, r.billing_state, r.billing_country, r.postal_code,
			       r.created_date, r.modified_date, r.is_active, r.is_deleted
			FROM records r
			JOIN (
				SELECT MAX(r.row_num) AS row_num
				FROM records r
				WHERE ` + whereSQL + `
				GROUP BY CASE
					WHEN COALESCE(r.id, '') <> '' THEN r.id
					ELSE printf('row:%d', r.row_num)
				END
			) d ON d.row_num = r.row_num
			ORDER BY ` + orderBy + `
			LIMIT ? OFFSET ?`
	} else {
		countSQL = "SELECT COUNT(1) FROM records r WHERE " + whereSQL
		querySQL = `
			SELECT r.row_num, r.id, r.name, COALESCE(r.phone, ''), COALESCE(r.has_sobject_log, 0), COALESCE(r.has_flash_message, 0), r.type, r.status, r.segment, r.sales_channel,
			       r.billing_city, r.billing_state, r.billing_country, r.postal_code,
			       r.created_date, r.modified_date, r.is_active, r.is_deleted
			FROM records r
			WHERE ` + whereSQL + `
			ORDER BY ` + orderBy + `
			LIMIT ? OFFSET ?`
	}

	var total int64
	if err := s.queryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return QueryResult{}, err
	}

	queryArgs := append(append(make([]any, 0, len(args)+2), args...), params.Limit, params.Offset)
	rows, err := s.queryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	out := QueryResult{
		Records: make([]Record, 0, params.Limit),
		Total:   total,
		Limit:   params.Limit,
		Offset:  params.Offset,
	}

	for rows.Next() {
		var rec Record
		var activeInt int
		var deletedInt int
		var hasLogInt int
		var hasFlashInt int
		if err := rows.Scan(
			&rec.RowNum,
			&rec.ID,
			&rec.Name,
			&rec.Phone,
			&hasLogInt,
			&hasFlashInt,
			&rec.Type,
			&rec.Status,
			&rec.Segment,
			&rec.SalesChannel,
			&rec.BillingCity,
			&rec.BillingState,
			&rec.BillingCountry,
			&rec.PostalCode,
			&rec.CreatedDate,
			&rec.ModifiedDate,
			&activeInt,
			&deletedInt,
		); err != nil {
			return QueryResult{}, err
		}
		rec.IsActive = activeInt == 1
		rec.IsDeleted = deletedInt == 1
		rec.HasSObjectLog = hasLogInt == 1
		rec.HasFlashMsg = hasFlashInt == 1
		out.Records = append(out.Records, rec)
	}

	return out, rows.Err()
}

func (s *Store) RecordJSON(ctx context.Context, rowNum int64) (json.RawMessage, error) {
	var offset int64
	var lineLen int64
	err := s.queryRowContext(ctx, `SELECT file_offset, line_length FROM records WHERE row_num = ?`, rowNum).
		Scan(&offset, &lineLen)
	if err != nil {
		return nil, err
	}

	if lineLen <= 0 {
		return nil, fmt.Errorf("invalid line length %d for row %d", lineLen, rowNum)
	}

	buf := make([]byte, lineLen)
	n, err := s.datasetFile.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	raw := bytes.TrimRight(buf[:n], "\r\n")
	if !json.Valid(raw) {
		raw, _ = json.Marshal(map[string]string{"raw": string(raw)})
	}
	return append(json.RawMessage(nil), raw...), nil
}

func (s *Store) RowsHasSObjectLog(ctx context.Context, rowNums []int64) (map[int64]bool, error) {
	return s.rowsHasNonEmptyJSONField(ctx, rowNums, "SObjectLog__c")
}

func (s *Store) RowsHasFlashMessage(ctx context.Context, rowNums []int64) (map[int64]bool, error) {
	out := make(map[int64]bool, len(rowNums))
	if len(rowNums) == 0 {
		return out, nil
	}

	seen := make(map[int64]struct{}, len(rowNums))
	uniq := make([]int64, 0, len(rowNums))
	for _, n := range rowNums {
		if n <= 0 {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		uniq = append(uniq, n)
	}
	if len(uniq) == 0 {
		return out, nil
	}

	placeholders := make([]string, 0, len(uniq))
	args := make([]any, 0, len(uniq))
	for _, n := range uniq {
		placeholders = append(placeholders, "?")
		args = append(args, n)
	}

	query := `SELECT row_num, has_flash_message FROM records WHERE row_num IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := s.queryContext(ctx, query, args...)
	if err != nil {
		// Backward compatibility for older indexes without the column.
		if strings.Contains(strings.ToLower(err.Error()), "no such column") {
			return s.rowsHasNonEmptyJSONField(ctx, rowNums, "Flash_Message__c")
		}
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rowNum int64
		var hasFlash int
		if err := rows.Scan(&rowNum, &hasFlash); err != nil {
			return nil, err
		}
		out[rowNum] = hasFlash == 1
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, n := range uniq {
		if _, ok := out[n]; !ok {
			out[n] = false
		}
	}
	return out, nil
}

func (s *Store) RowsBestPhone(ctx context.Context, rowNums []int64) (map[int64]string, error) {
	out := make(map[int64]string, len(rowNums))
	if len(rowNums) == 0 {
		return out, nil
	}

	seen := make(map[int64]struct{}, len(rowNums))
	uniq := make([]int64, 0, len(rowNums))
	for _, n := range rowNums {
		if n <= 0 {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		uniq = append(uniq, n)
	}
	if len(uniq) == 0 {
		return out, nil
	}

	placeholders := make([]string, 0, len(uniq))
	args := make([]any, 0, len(uniq))
	for _, n := range uniq {
		placeholders = append(placeholders, "?")
		args = append(args, n)
	}

	query := `SELECT row_num, COALESCE(phone, ''), file_offset, line_length FROM records WHERE row_num IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := s.queryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type fileRef struct {
		Offset int64
		Len    int64
	}
	refs := make(map[int64]fileRef, len(uniq))
	for rows.Next() {
		var rowNum int64
		var phone string
		var offset int64
		var lineLen int64
		if err := rows.Scan(&rowNum, &phone, &offset, &lineLen); err != nil {
			return nil, err
		}
		phone = strings.TrimSpace(phone)
		if phone != "" {
			out[rowNum] = phone
			continue
		}
		refs[rowNum] = fileRef{Offset: offset, Len: lineLen}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, n := range uniq {
		if _, ok := out[n]; ok {
			continue
		}
		ref, ok := refs[n]
		if !ok || ref.Len <= 0 {
			out[n] = ""
			continue
		}

		buf := make([]byte, ref.Len)
		readN, readErr := s.datasetFile.ReadAt(buf, ref.Offset)
		if readErr != nil && readErr != io.EOF {
			out[n] = ""
			continue
		}
		raw := bytes.TrimRight(buf[:readN], "\r\n")
		if !json.Valid(raw) {
			out[n] = ""
			continue
		}

		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			out[n] = ""
			continue
		}
		out[n] = pickFirstPhone(doc)
	}

	return out, nil
}

func (s *Store) rowsHasNonEmptyJSONField(ctx context.Context, rowNums []int64, field string) (map[int64]bool, error) {
	out := make(map[int64]bool, len(rowNums))
	if len(rowNums) == 0 {
		return out, nil
	}

	seen := make(map[int64]struct{}, len(rowNums))
	uniq := make([]int64, 0, len(rowNums))
	for _, n := range rowNums {
		if n <= 0 {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		uniq = append(uniq, n)
	}
	if len(uniq) == 0 {
		return out, nil
	}

	placeholders := make([]string, 0, len(uniq))
	args := make([]any, 0, len(uniq))
	for _, n := range uniq {
		placeholders = append(placeholders, "?")
		args = append(args, n)
	}

	query := `SELECT row_num, file_offset, line_length FROM records WHERE row_num IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := s.queryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type fileRef struct {
		Offset int64
		Len    int64
	}
	refs := make(map[int64]fileRef, len(uniq))
	for rows.Next() {
		var rowNum, offset, lineLen int64
		if err := rows.Scan(&rowNum, &offset, &lineLen); err != nil {
			return nil, err
		}
		refs[rowNum] = fileRef{Offset: offset, Len: lineLen}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, n := range uniq {
		ref, ok := refs[n]
		if !ok || ref.Len <= 0 {
			out[n] = false
			continue
		}

		buf := make([]byte, ref.Len)
		readN, readErr := s.datasetFile.ReadAt(buf, ref.Offset)
		if readErr != nil && readErr != io.EOF {
			out[n] = false
			continue
		}

		raw := bytes.TrimRight(buf[:readN], "\r\n")
		if !json.Valid(raw) {
			out[n] = false
			continue
		}

		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			out[n] = false
			continue
		}
		out[n] = strings.TrimSpace(anyToString(doc[field])) != ""
	}

	return out, nil
}

func pickFirstPhone(doc map[string]any) string {
	keys := []string{
		"Phone",
		"MobilePhone",
		"HomePhone",
		"OtherPhone",
		"PersonPhone",
		"PersonMobilePhone",
		"PersonHomePhone",
		"PersonOtherPhone",
	}
	for _, key := range keys {
		v := strings.TrimSpace(anyToString(doc[key]))
		if v != "" {
			return v
		}
	}
	return ""
}

func applyRuntimePragmas(db *sql.DB) error {
	// PostgreSQL doesn't use pragmas like SQLite; this is a no-op for compatibility
	return nil
}

func applyIndexingPragmas(db *sql.DB, fast bool) error {
	// PostgreSQL doesn't use pragmas like SQLite; this is a no-op for compatibility
	return nil
}

func postgresPlaceholders(query string) string {
	var b strings.Builder
	b.Grow(len(query) + 16)
	n := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			n++
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

func (s *Store) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, postgresPlaceholders(query), args...)
}

func (s *Store) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, postgresPlaceholders(query), args...)
}

func (s *Store) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, postgresPlaceholders(query), args...)
}

func (s *Store) isIndexCurrent(ctx context.Context) (bool, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'metadata'`).Scan(&exists); err != nil {
		return false, err
	}
	if exists == 0 {
		return false, nil
	}

	size, err := s.metaValue(ctx, "source_size")
	if err != nil {
		return false, nil
	}
	mtime, err := s.metaValue(ctx, "source_mtime_unix")
	if err != nil {
		return false, nil
	}
	version, err := s.metaValue(ctx, "index_version")
	if err != nil {
		return false, nil
	}

	fileInfo, err := os.Stat(s.datasetPath)
	if err != nil {
		return false, err
	}

	if version != indexVersion {
		return false, nil
	}
	if size != strconv.FormatInt(fileInfo.Size(), 10) {
		return false, nil
	}
	if mtime != strconv.FormatInt(fileInfo.ModTime().Unix(), 10) {
		return false, nil
	}

	var recordsTable int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'records'`).Scan(&recordsTable); err != nil {
		return false, err
	}
	return recordsTable == 1, nil
}

func (s *Store) metadataTableExists(ctx context.Context) (bool, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'metadata'`).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (s *Store) readResumeCheckpoint(ctx context.Context, fileInfo os.FileInfo, cfg indexConfig) (int64, int64, bool, error) {
	exists, err := s.metadataTableExists(ctx)
	if err != nil || !exists {
		return 0, 0, false, err
	}

	required := []string{
		"build_in_progress",
		"build_index_version",
		"build_source_size",
		"build_source_mtime_unix",
		"build_fts_broad",
		"build_index_json_fields",
		"build_last_row",
		"build_last_offset",
	}
	values := make(map[string]string, len(required))
	for _, key := range required {
		v, readErr := s.metaValue(ctx, key)
		if readErr != nil {
			if errors.Is(readErr, sql.ErrNoRows) {
				return 0, 0, false, nil
			}
			return 0, 0, false, readErr
		}
		values[key] = v
	}

	if values["build_in_progress"] != "1" {
		return 0, 0, false, nil
	}
	if values["build_index_version"] != indexVersion {
		return 0, 0, false, nil
	}
	if values["build_source_size"] != strconv.FormatInt(fileInfo.Size(), 10) {
		return 0, 0, false, nil
	}
	if values["build_source_mtime_unix"] != strconv.FormatInt(fileInfo.ModTime().Unix(), 10) {
		return 0, 0, false, nil
	}
	if values["build_fts_broad"] != strconv.FormatBool(cfg.FTSBroad) {
		return 0, 0, false, nil
	}
	if values["build_index_json_fields"] != strconv.FormatBool(cfg.IndexJSONFields) {
		return 0, 0, false, nil
	}

	lastRow, err := strconv.ParseInt(values["build_last_row"], 10, 64)
	if err != nil || lastRow < 0 {
		return 0, 0, false, nil
	}
	lastOffset, err := strconv.ParseInt(values["build_last_offset"], 10, 64)
	if err != nil || lastOffset < 0 || lastOffset > fileInfo.Size() {
		return 0, 0, false, nil
	}
	return lastRow, lastOffset, true, nil
}

func upsertMetadataTx(ctx context.Context, tx *sql.Tx, key, value string) error {
	_, err := tx.ExecContext(ctx, postgresPlaceholders(`
		INSERT INTO metadata(key, value) VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value
	`), key, value)
	return err
}

func upsertMetadataMapTx(ctx context.Context, tx *sql.Tx, values map[string]string) error {
	for k, v := range values {
		if err := upsertMetadataTx(ctx, tx, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) rebuildIndex(ctx context.Context) error {
	start := time.Now()
	fmt.Printf("rebuilding index from %s...\n", s.datasetPath)
	s.updateStatus(func(st *IndexStatus) {
		st.Step = "prepare_schema"
		st.Message = "creating schema"
		st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	})

	if err := applyIndexingPragmas(s.db, s.fastIndex); err != nil {
		return err
	}
	defer func() {
		_ = applyRuntimePragmas(s.db)
	}()

	cfg := loadIndexConfig(s.fastIndex, s.ftsBroad)
	fmt.Printf(
		"index config: fast=%t fts_broad=%t workers=%d, commit_batch=%d, json_batch=%d, index_json_fields=%t\n",
		s.fastIndex,
		cfg.FTSBroad,
		cfg.ParseWorkers,
		cfg.CommitBatchRows,
		cfg.JSONFieldInsertBatch,
		cfg.IndexJSONFields,
	)
	fileInfo, err := os.Stat(s.datasetPath)
	if err != nil {
		return err
	}

	var resumeFromRow int64
	var resumeFromOffset int64
	var resumeRowsIndexed int64
	var resumeParseErrors int64
	lastRow, lastOffset, canResume, err := s.readResumeCheckpoint(ctx, fileInfo, cfg)
	if err != nil {
		return err
	}
	if canResume {
		resumeFromRow = lastRow
		resumeFromOffset = lastOffset
		if v, readErr := s.metaValue(ctx, "build_rows_indexed"); readErr == nil {
			resumeRowsIndexed, _ = strconv.ParseInt(v, 10, 64)
		}
		if v, readErr := s.metaValue(ctx, "build_parse_errors"); readErr == nil {
			resumeParseErrors, _ = strconv.ParseInt(v, 10, 64)
		}
		fmt.Printf("resume checkpoint found: row=%d offset=%d\n", resumeFromRow, resumeFromOffset)
	} else {
		schemaSQL := `
			DROP TABLE IF EXISTS records CASCADE;
			DROP TABLE IF EXISTS record_json_fields;
			DROP TABLE IF EXISTS metadata;

			CREATE TABLE records (
				row_num BIGSERIAL PRIMARY KEY,
				id TEXT,
				name TEXT,
				email TEXT,
				phone TEXT,
				has_sobject_log INTEGER NOT NULL,
				has_flash_message INTEGER NOT NULL,
				search_blob TEXT,
				type TEXT,
				status TEXT,
				segment TEXT,
				sales_channel TEXT,
				billing_city TEXT,
				billing_state TEXT,
				billing_country TEXT,
				postal_code TEXT,
				created_date TEXT,
				modified_date TEXT,
				is_active INTEGER NOT NULL,
				is_deleted INTEGER NOT NULL,
				file_offset BIGINT NOT NULL,
				line_length BIGINT NOT NULL
			);

			CREATE TABLE record_json_fields (
				row_num BIGINT NOT NULL,
				path TEXT NOT NULL,
				value_text TEXT NOT NULL,
				value_type TEXT NOT NULL
			);

			CREATE TABLE metadata (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL
			);
		`
		if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
			return err
		}
	}
	s.updateStatus(func(st *IndexStatus) {
		st.Step = "ingest_rows"
		if canResume {
			st.Message = fmt.Sprintf("resuming import at row %d", resumeFromRow+1)
		} else {
			st.Message = "importing dataset rows"
		}
		st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	})

	metaInitTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	metaInit := map[string]string{
		"build_in_progress":       "1",
		"build_index_version":     indexVersion,
		"build_source_size":       strconv.FormatInt(fileInfo.Size(), 10),
		"build_source_mtime_unix": strconv.FormatInt(fileInfo.ModTime().Unix(), 10),
		"build_fts_broad":         strconv.FormatBool(cfg.FTSBroad),
		"build_index_json_fields": strconv.FormatBool(cfg.IndexJSONFields),
		"build_last_row":          strconv.FormatInt(resumeFromRow, 10),
		"build_last_offset":       strconv.FormatInt(resumeFromOffset, 10),
	}
	if err := upsertMetadataMapTx(ctx, metaInitTx, metaInit); err != nil {
		metaInitTx.Rollback()
		return err
	}
	if err := metaInitTx.Commit(); err != nil {
		return err
	}

	f, err := os.Open(s.datasetPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if resumeFromOffset > 0 {
		if _, err := f.Seek(resumeFromOffset, io.SeekStart); err != nil {
			return err
		}
	}
	reader := bufio.NewReaderSize(f, 8*1024*1024)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	commitBatch := func() error {
		if err := tx.Commit(); err != nil {
			return err
		}
		next, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		tx = next
		return nil
	}

	insertRecordSQL := `
		INSERT INTO records(
			row_num, id, name, email, phone, has_sobject_log, has_flash_message, search_blob, type, status, segment, sales_channel,
			billing_city, billing_state, billing_country, postal_code,
			created_date, modified_date, is_active, is_deleted,
			file_offset, line_length
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	type parseTask struct {
		RowNum     int64
		FileOffset int64
		LineLen    int64
		Payload    []byte
	}
	type jsonFieldWrite struct {
		RowNum    int64
		Path      string
		ValueText string
		ValueType string
	}
	type parseResult struct {
		RowNum     int64
		FileOffset int64
		LineLen    int64
		Src        sourceRecord
		Active     int
		Deleted    int
		Fields     []jsonIndexedField
		ParseError bool
	}
	type writeOutcome struct {
		InsertedRows int64
		ParseErrors  int64
		Err          error
	}

	ingestCtx, cancelIngest := context.WithCancel(ctx)
	defer cancelIngest()

	workerCount := cfg.ParseWorkers
	tasks := make(chan parseTask, workerCount*8)
	results := make(chan parseResult, workerCount*8)
	outcomes := make(chan writeOutcome, 1)

	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()

			localFields := make([]jsonIndexedField, 0, 32)
			for {
				select {
				case <-ingestCtx.Done():
					return
				case task, ok := <-tasks:
					if !ok {
						return
					}

					var doc any
					if err := json.Unmarshal(task.Payload, &doc); err != nil {
						select {
						case results <- parseResult{
							RowNum:     task.RowNum,
							FileOffset: task.FileOffset,
							LineLen:    task.LineLen,
							ParseError: true,
						}:
						case <-ingestCtx.Done():
						}
						continue
					}

					obj, ok := doc.(map[string]any)
					if !ok {
						select {
						case results <- parseResult{
							RowNum:     task.RowNum,
							FileOffset: task.FileOffset,
							LineLen:    task.LineLen,
							ParseError: true,
						}:
						case <-ingestCtx.Done():
						}
						continue
					}

					src := sourceRecord{
						ID:             anyToString(obj["Id"]),
						Name:           anyToString(obj["Name"]),
						Email:          anyToString(obj["Email"]),
						Phone:          anyToString(obj["Phone"]),
						MobilePhone:    anyToString(obj["MobilePhone"]),
						HomePhone:      anyToString(obj["HomePhone"]),
						OtherPhone:     anyToString(obj["OtherPhone"]),
						PersonPhone:    anyToString(obj["PersonPhone"]),
						PersonMobile:   anyToString(obj["PersonMobilePhone"]),
						PersonHome:     anyToString(obj["PersonHomePhone"]),
						PersonOther:    anyToString(obj["PersonOtherPhone"]),
						SObjectLog:     anyToString(obj["SObjectLog__c"]),
						FlashMessage:   anyToString(obj["Flash_Message__c"]),
						Type:           anyToString(obj["Type"]),
						AttrType:       anyToString(obj["attributes_type"]),
						Status:         anyToString(obj["vlocity_cmt__Status__c"]),
						Segment:        anyToString(obj["Segment__c"]),
						SalesChannel:   anyToString(obj["Sales_Channel__c"]),
						BillingCity:    anyToString(obj["BillingCity"]),
						BillingState:   anyToString(obj["BillingState"]),
						BillingCountry: anyToString(obj["BillingCountry"]),
						CountryCode:    anyToString(obj["CountryCode__c"]),
						PostalCode:     anyToString(obj["BillingPostalCode"]),
						CreatedDate:    anyToString(obj["CreatedDate"]),
						ModifiedDate:   anyToString(obj["LastModifiedDate"]),
					}

					active := 0
					if parseAnyBool(obj["IsActive"]) {
						active = 1
					}
					deleted := 0
					if parseAnyBool(obj["IsDeleted"]) {
						deleted = 1
					}
					if src.Type == "" {
						src.Type = src.AttrType
					}
					if src.BillingCountry == "" {
						src.BillingCountry = src.CountryCode
					}
					if src.Phone == "" {
						src.Phone = src.MobilePhone
					}
					if src.Phone == "" {
						src.Phone = src.HomePhone
					}
					if src.Phone == "" {
						src.Phone = src.OtherPhone
					}
					if src.Phone == "" {
						src.Phone = src.PersonPhone
					}
					if src.Phone == "" {
						src.Phone = src.PersonMobile
					}
					if src.Phone == "" {
						src.Phone = src.PersonHome
					}
					if src.Phone == "" {
						src.Phone = src.PersonOther
					}
					if cfg.FTSBroad {
						src.SearchBlob = buildBroadSearchBlob(obj)
					}

					localFields = localFields[:0]
					if cfg.IndexJSONFields {
						collectJSONIndexedFields("", doc, &localFields)
					}
					fieldsCopy := append([]jsonIndexedField(nil), localFields...)

					res := parseResult{
						RowNum:     task.RowNum,
						FileOffset: task.FileOffset,
						LineLen:    task.LineLen,
						Src:        src,
						Active:     active,
						Deleted:    deleted,
						Fields:     fieldsCopy,
					}
					select {
					case results <- res:
					case <-ingestCtx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		workers.Wait()
		close(results)
	}()

	go func() {
		insertedRows := resumeRowsIndexed
		parseErrors := resumeParseErrors
		var processedLines int64
		var lastCommittedRow = resumeFromRow
		var lastCommittedOffset = resumeFromOffset

		recStmt, err := tx.PrepareContext(ingestCtx, insertRecordSQL)
		if err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		// FTS is not used with PostgreSQL - removed for compatibility
		pendingJSONFields := make([]jsonFieldWrite, 0, cfg.JSONFieldInsertBatch*2)

		closeStmts := func() error {
			return recStmt.Close()
		}
		reopenStmts := func() error {
			var prepareErr error
			recStmt, prepareErr = tx.PrepareContext(ingestCtx, insertRecordSQL)
			if prepareErr != nil {
				return prepareErr
			}
			return nil
		}
		flushJSONFieldChunk := func(chunk []jsonFieldWrite) error {
			if len(chunk) == 0 {
				return nil
			}

			var sqlBuilder strings.Builder
			sqlBuilder.Grow(96 + len(chunk)*12)
			sqlBuilder.WriteString("INSERT INTO record_json_fields(row_num, path, value_text, value_type) VALUES ")
			args := make([]any, 0, len(chunk)*4)
			for i, item := range chunk {
				if i > 0 {
					sqlBuilder.WriteString(",")
				}
				sqlBuilder.WriteString("(?, ?, ?, ?)")
				args = append(args, item.RowNum, item.Path, item.ValueText, item.ValueType)
			}

			_, err := tx.ExecContext(ingestCtx, sqlBuilder.String(), args...)
			return err
		}
		flushAllPendingJSONFields := func() error {
			for len(pendingJSONFields) > 0 {
				end := cfg.JSONFieldInsertBatch
				if len(pendingJSONFields) < end {
					end = len(pendingJSONFields)
				}
				if err := flushJSONFieldChunk(pendingJSONFields[:end]); err != nil {
					return err
				}
				pendingJSONFields = pendingJSONFields[end:]
			}
			return nil
		}
		writeCheckpoint := func() error {
			values := map[string]string{
				"build_in_progress":  "1",
				"build_last_row":     strconv.FormatInt(lastCommittedRow, 10),
				"build_last_offset":  strconv.FormatInt(lastCommittedOffset, 10),
				"build_parse_errors": strconv.FormatInt(parseErrors, 10),
				"build_rows_indexed": strconv.FormatInt(insertedRows, 10),
			}
			return upsertMetadataMapTx(ingestCtx, tx, values)
		}

		pendingByRow := make(map[int64]parseResult, workerCount*8)
		nextRowToWrite := resumeFromRow + 1
		writeOne := func(res parseResult) error {
			processedLines++
			if res.ParseError {
				parseErrors++
			} else {
				if _, err := recStmt.ExecContext(
					ingestCtx,
					res.RowNum,
					res.Src.ID,
					res.Src.Name,
					res.Src.Email,
					res.Src.Phone,
					boolToInt(strings.TrimSpace(res.Src.SObjectLog) != ""),
					boolToInt(strings.TrimSpace(res.Src.FlashMessage) != ""),
					res.Src.SearchBlob,
					res.Src.Type,
					res.Src.Status,
					res.Src.Segment,
					res.Src.SalesChannel,
					res.Src.BillingCity,
					res.Src.BillingState,
					res.Src.BillingCountry,
					res.Src.PostalCode,
					res.Src.CreatedDate,
					res.Src.ModifiedDate,
					res.Active,
					res.Deleted,
					res.FileOffset,
					res.LineLen,
				); err != nil {
					return err
				}

				// FTS insert removed - PostgreSQL uses ILIKE instead

				if cfg.IndexJSONFields {
					for _, field := range res.Fields {
						pendingJSONFields = append(pendingJSONFields, jsonFieldWrite{
							RowNum:    res.RowNum,
							Path:      field.Path,
							ValueText: field.ValueText,
							ValueType: field.ValueType,
						})
						if len(pendingJSONFields) >= cfg.JSONFieldInsertBatch {
							if err := flushJSONFieldChunk(pendingJSONFields[:cfg.JSONFieldInsertBatch]); err != nil {
								return err
							}
							pendingJSONFields = pendingJSONFields[cfg.JSONFieldInsertBatch:]
						}
					}
				}
				insertedRows++
			}

			lastCommittedRow = res.RowNum
			lastCommittedOffset = res.FileOffset + res.LineLen

			if cfg.CommitBatchRows > 0 && processedLines%cfg.CommitBatchRows == 0 {
				if err := flushAllPendingJSONFields(); err != nil {
					return err
				}
				if err := writeCheckpoint(); err != nil {
					return err
				}
				if err := closeStmts(); err != nil {
					return err
				}
				if err := commitBatch(); err != nil {
					return err
				}
				if err := reopenStmts(); err != nil {
					return err
				}
			}

			if processedLines%cfg.ProgressEvery == 0 {
				fmt.Printf("processed %d lines...\n", resumeFromRow+processedLines)
				s.updateStatus(func(st *IndexStatus) {
					st.RowsIndexed = insertedRows
					st.ParseErrors = parseErrors
					st.Step = "ingest_rows"
					st.Message = "importing dataset rows"
					st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
				})
			}
			return nil
		}

		for res := range results {
			pendingByRow[res.RowNum] = res
			for {
				nextRes, ok := pendingByRow[nextRowToWrite]
				if !ok {
					break
				}
				delete(pendingByRow, nextRowToWrite)
				if err := writeOne(nextRes); err != nil {
					cancelIngest()
					outcomes <- writeOutcome{Err: err}
					return
				}
				nextRowToWrite++
			}
		}

		if err := flushAllPendingJSONFields(); err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		if err := writeCheckpoint(); err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		if err := closeStmts(); err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		if err := tx.Commit(); err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		outcomes <- writeOutcome{
			InsertedRows: insertedRows,
			ParseErrors:  parseErrors,
		}
	}()

	var lineNum = resumeFromRow
	var fileOffset = resumeFromOffset
	var readerErr error
	var writerOutcome writeOutcome
	writerOutcomeReady := false

readLoop:
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) == 0 && readErr == io.EOF {
			break
		}
		if len(line) == 0 && readErr != nil {
			readerErr = readErr
			cancelIngest()
			break
		}

		lineNum++
		lineLen := int64(len(line))
		payload := bytes.TrimRight(line, "\r\n")

		task := parseTask{
			RowNum:     lineNum,
			FileOffset: fileOffset,
			LineLen:    lineLen,
			Payload:    append([]byte(nil), payload...),
		}
		select {
		case tasks <- task:
		case writerOutcome = <-outcomes:
			writerOutcomeReady = true
			cancelIngest()
			break readLoop
		case <-ingestCtx.Done():
			break readLoop
		}

		fileOffset += lineLen

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			readerErr = readErr
			cancelIngest()
			break
		}
	}
	close(tasks)

	if !writerOutcomeReady {
		writerOutcome = <-outcomes
		writerOutcomeReady = true
	}

	if writerOutcome.Err != nil {
		_ = tx.Rollback()
		return writerOutcome.Err
	}
	if readerErr != nil {
		_ = tx.Rollback()
		return readerErr
	}

	insertedRows := writerOutcome.InsertedRows
	parseErrors := writerOutcome.ParseErrors
	s.updateStatus(func(st *IndexStatus) {
		st.RowsIndexed = insertedRows
		st.ParseErrors = parseErrors
		st.Step = "build_fts"
		st.Message = "building full-text index"
		st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	})

	// FTS is not used with PostgreSQL - uses ILIKE instead

	indexSQL := `
		CREATE INDEX idx_records_id ON records(id);
		CREATE INDEX idx_records_phone ON records(phone);
		CREATE INDEX idx_records_haslog ON records(has_sobject_log);
		CREATE INDEX idx_records_hasflash ON records(has_flash_message);
		CREATE INDEX idx_records_type ON records(type);
		CREATE INDEX idx_records_status ON records(status);
		CREATE INDEX idx_records_country ON records(billing_country);
		CREATE INDEX idx_records_city ON records(billing_city);
		CREATE INDEX idx_records_modified ON records(modified_date);
		CREATE INDEX idx_records_active ON records(is_active);
		CREATE INDEX idx_json_fields_path_value ON record_json_fields(path, value_text);
		CREATE INDEX idx_json_fields_value ON record_json_fields(value_text);
		CREATE INDEX idx_json_fields_row ON record_json_fields(row_num);
	`
	if _, err := s.db.ExecContext(ctx, indexSQL); err != nil {
		return err
	}
	s.updateStatus(func(st *IndexStatus) {
		st.Step = "finalize"
		st.Message = "writing metadata"
		st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	})

	meta := map[string]string{
		"index_version":           indexVersion,
		"source_size":             strconv.FormatInt(fileInfo.Size(), 10),
		"source_mtime_unix":       strconv.FormatInt(fileInfo.ModTime().Unix(), 10),
		"indexed_at":              time.Now().UTC().Format(time.RFC3339),
		"row_count":               strconv.FormatInt(insertedRows, 10),
		"parse_errors":            strconv.FormatInt(parseErrors, 10),
		"build_in_progress":       "0",
		"build_last_row":          strconv.FormatInt(lineNum, 10),
		"build_last_offset":       strconv.FormatInt(fileOffset, 10),
		"build_rows_indexed":      strconv.FormatInt(insertedRows, 10),
		"build_parse_errors":      strconv.FormatInt(parseErrors, 10),
		"build_fts_broad":         strconv.FormatBool(cfg.FTSBroad),
		"build_index_json_fields": strconv.FormatBool(cfg.IndexJSONFields),
	}

	metaTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := upsertMetadataMapTx(ctx, metaTx, meta); err != nil {
		metaTx.Rollback()
		return err
	}
	if err := metaTx.Commit(); err != nil {
		return err
	}

	fmt.Printf("index rebuild complete in %s (rows=%d, parse_errors=%d)\n", time.Since(start).Round(time.Second), lineNum, parseErrors)
	s.updateStatus(func(st *IndexStatus) {
		st.RowsIndexed = insertedRows
		st.ParseErrors = parseErrors
		st.Step = "ready"
		st.Message = "index rebuild complete"
		st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	})
	return nil
}

func (s *Store) metaValue(ctx context.Context, key string) (string, error) {
	var value string
	if err := s.queryRowContext(ctx, `SELECT value FROM metadata WHERE key = ?`, key).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func (s *Store) topCounts(ctx context.Context, field string, limit int) ([]CountBucket, error) {
	column, ok := map[string]string{
		"type":            "type",
		"status":          "status",
		"billing_country": "billing_country",
		"billing_city":    "billing_city",
	}[field]
	if !ok {
		return nil, fmt.Errorf("unsupported field %q", field)
	}

	query := fmt.Sprintf(`
		SELECT %s AS value, COUNT(1) AS count
		FROM records
		WHERE COALESCE(%s, '') <> ''
		GROUP BY %s
		ORDER BY count DESC, value ASC
		LIMIT ?`, column, column, column)

	rows, err := s.queryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]CountBucket, 0, limit)
	for rows.Next() {
		var b CountBucket
		if err := rows.Scan(&b.Value, &b.Count); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) distinctValues(ctx context.Context, field string, limit int) ([]string, error) {
	column, ok := map[string]string{
		"type":            "type",
		"status":          "status",
		"billing_country": "billing_country",
		"billing_city":    "billing_city",
	}[field]
	if !ok {
		return nil, fmt.Errorf("unsupported field %q", field)
	}

	query := fmt.Sprintf(`
		SELECT %s AS value
		FROM records
		WHERE COALESCE(%s, '') <> ''
		GROUP BY %s
		ORDER BY COUNT(1) DESC, value ASC
		LIMIT ?`, column, column, column)

	rows, err := s.queryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) JSONPaths(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 2000 {
		limit = 2000
	}

	rows, err := s.queryContext(ctx, `
		SELECT path
		FROM record_json_fields
		GROUP BY path
		ORDER BY COUNT(1) DESC, path ASC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	return out, rows.Err()
}

func (s *Store) AnalyticsFields(ctx context.Context) ([]AnalyticsField, error) {
	specs, err := s.analyticsFieldSpecs(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]AnalyticsField, 0, len(specs))
	for _, spec := range specs {
		out = append(out, AnalyticsField{Name: spec.Name, Label: spec.Label})
	}
	return out, nil
}

func (s *Store) AnalyticsDistribution(ctx context.Context, field, filter string, limit int, notEmpty bool) (AnalyticsDistribution, error) {
	if limit <= 0 {
		limit = 25
	}
	if limit > 500 {
		limit = 500
	}

	spec, err := s.analyticsFieldSpec(ctx, field)
	if err != nil {
		return AnalyticsDistribution{}, err
	}

	filter = strings.TrimSpace(filter)
	whereParts := make([]string, 0, 2)
	filterArgs := make([]any, 0, 2)
	if filter != "" {
		whereParts = append(whereParts, "value LIKE ?")
		filterArgs = append(filterArgs, "%"+filter+"%")
	}
	if notEmpty {
		whereParts = append(whereParts, "value <> '(empty)'")
	}
	whereSQL := ""
	if len(whereParts) > 0 {
		whereSQL = " WHERE " + strings.Join(whereParts, " AND ")
	}

	sourceSQL := fmt.Sprintf("SELECT %s AS value FROM records", spec.Expr)

	var totalRows int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM records`).Scan(&totalRows); err != nil {
		return AnalyticsDistribution{}, err
	}

	matchedSQL := fmt.Sprintf(
		"SELECT COUNT(1) FROM (%s) values_view%s",
		sourceSQL,
		whereSQL,
	)
	var matchedRows int64
	if err := s.queryRowContext(ctx, matchedSQL, filterArgs...).Scan(&matchedRows); err != nil {
		return AnalyticsDistribution{}, err
	}

	distinctSQL := fmt.Sprintf(
		"SELECT COUNT(1) FROM (SELECT value FROM (%s) values_view%s GROUP BY value) grouped_values",
		sourceSQL,
		whereSQL,
	)
	var distinctCount int64
	if err := s.queryRowContext(ctx, distinctSQL, filterArgs...).Scan(&distinctCount); err != nil {
		return AnalyticsDistribution{}, err
	}

	distributionSQL := fmt.Sprintf(`
		SELECT value, COUNT(1) AS count
		FROM (%s) values_view
		%s
		GROUP BY value
		ORDER BY count DESC, value ASC
		LIMIT ?`, sourceSQL, whereSQL)
	args := append(append(make([]any, 0, len(filterArgs)+1), filterArgs...), limit)
	rows, err := s.queryContext(ctx, distributionSQL, args...)
	if err != nil {
		return AnalyticsDistribution{}, err
	}
	defer rows.Close()

	out := AnalyticsDistribution{
		Field:         spec.Name,
		Filter:        filter,
		Limit:         limit,
		TotalRows:     totalRows,
		MatchedRows:   matchedRows,
		DistinctCount: distinctCount,
		Buckets:       make([]AnalyticsBucket, 0, limit),
	}

	for rows.Next() {
		var bucket AnalyticsBucket
		if err := rows.Scan(&bucket.Value, &bucket.Count); err != nil {
			return AnalyticsDistribution{}, err
		}
		if out.MatchedRows > 0 {
			bucket.Percentage = (float64(bucket.Count) / float64(out.MatchedRows)) * 100
		}
		out.Buckets = append(out.Buckets, bucket)
	}

	if err := rows.Err(); err != nil {
		return AnalyticsDistribution{}, err
	}
	return out, nil
}

func (s *Store) AnalyticsCount(ctx context.Context, field, value string) (AnalyticsCountResult, error) {
	spec, err := s.analyticsFieldSpec(ctx, field)
	if err != nil {
		return AnalyticsCountResult{}, err
	}

	query := fmt.Sprintf(
		"SELECT COUNT(1) FROM (SELECT %s AS value FROM records) values_view WHERE value = ?",
		spec.Expr,
	)

	var count int64
	if err := s.queryRowContext(ctx, query, value).Scan(&count); err != nil {
		return AnalyticsCountResult{}, err
	}
	return AnalyticsCountResult{
		Field: spec.Name,
		Value: value,
		Count: count,
	}, nil
}

func (s *Store) analyticsFieldSpec(ctx context.Context, field string) (analyticsFieldSpec, error) {
	field = strings.ToLower(strings.TrimSpace(field))
	specs, err := s.analyticsFieldSpecs(ctx)
	if err != nil {
		return analyticsFieldSpec{}, err
	}
	for _, spec := range specs {
		if spec.Name == field {
			return spec, nil
		}
	}
	return analyticsFieldSpec{}, fmt.Errorf("unsupported field %q", field)
}

func (s *Store) analyticsFieldSpecs(ctx context.Context) ([]analyticsFieldSpec, error) {
	rows, err := s.queryContext(ctx, `
		SELECT column_name as name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'records'
		AND column_name NOT IN ('row_num', 'file_offset', 'line_length')
		ORDER BY column_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	specs := make([]analyticsFieldSpec, 0, 12)
	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		specs = append(specs, analyticsFieldSpec{
			Name:  name,
			Label: analyticsFieldLabel(name),
			Expr:  analyticsValueExpr(name),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	specs = append(specs, analyticsFieldSpec{
		Name:  "email_domain",
		Label: analyticsFieldLabel("email_domain"),
		Expr: `CASE
			WHEN COALESCE(NULLIF(TRIM(CAST(email AS TEXT)), ''), '') = '' THEN '(empty)'
			WHEN INSTR(LOWER(TRIM(CAST(email AS TEXT))), '@') > 0
				THEN SUBSTR(LOWER(TRIM(CAST(email AS TEXT))), INSTR(LOWER(TRIM(CAST(email AS TEXT))), '@') + 1)
			ELSE '(invalid)'
		END`,
	})
	return specs, nil
}

func analyticsFieldLabel(name string) string {
	if override, ok := analyticsLabelOverrides[name]; ok {
		return override
	}

	parts := strings.Split(name, "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func analyticsValueExpr(column string) string {
	quoted := analyticsQuoteIdent(column)
	if column == "is_active" || column == "is_deleted" {
		return fmt.Sprintf("CASE WHEN %s = 1 THEN 'true' ELSE 'false' END", quoted)
	}
	return fmt.Sprintf("COALESCE(NULLIF(TRIM(CAST(%s AS TEXT)), ''), '(empty)')", quoted)
}

func analyticsQuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func anyToString(v any) string {
	switch value := v.(type) {
	case nil:
		return ""
	case string:
		return value
	case bool:
		if value {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func parseAnyBool(v any) bool {
	switch value := v.(type) {
	case bool:
		return value
	case float64:
		return value != 0
	case string:
		return parseBool(value)
	default:
		return false
	}
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func buildBroadSearchBlob(obj map[string]any) string {
	// Keep broad search practical: include likely contact/address/domain fields, cap size.
	preferredKeys := []string{
		"Email", "PersonEmail",
		"Phone", "MobilePhone", "HomePhone", "OtherPhone",
		"PersonPhone", "PersonMobilePhone", "PersonHomePhone", "PersonOtherPhone",
		"Fax",
		"Website", "Domain", "Domain__c",
		"BillingStreet", "ShippingStreet", "BillingPostalCode", "ShippingPostalCode",
		"BillingState", "ShippingState", "BillingCity", "ShippingCity",
		"BillingCountry", "ShippingCountry",
		"FirstName", "LastName", "Name",
		"Flash_Message__c",
	}

	parts := make([]string, 0, 64)
	seen := make(map[string]struct{}, 64)
	appendValue := func(v any, maxLen int) {
		s := strings.TrimSpace(anyToString(v))
		if s == "" {
			return
		}
		if maxLen <= 0 {
			maxLen = 256
		}
		if len(s) > maxLen {
			s = s[:maxLen]
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		parts = append(parts, s)
	}

	for _, key := range preferredKeys {
		if v, ok := obj[key]; ok {
			if key == "Flash_Message__c" {
				appendValue(stripHTMLForSearch(anyToString(v)), 1200)
				continue
			}
			appendValue(v, 256)
		}
	}

	// Catch additional top-level string fields that look searchable by keyword.
	for key, v := range obj {
		lk := strings.ToLower(key)
		if strings.Contains(lk, "email") ||
			strings.Contains(lk, "phone") ||
			strings.Contains(lk, "mobile") ||
			strings.Contains(lk, "fax") ||
			strings.Contains(lk, "website") ||
			strings.Contains(lk, "domain") ||
			strings.Contains(lk, "url") {
			appendValue(v, 256)
		}
	}

	blob := strings.Join(parts, " ")
	if len(blob) > 4096 {
		blob = blob[:4096]
	}
	return blob
}

var htmlTagPattern = regexp.MustCompile(`(?s)<[^>]*>`)

func stripHTMLForSearch(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, "<br>", "\n")
	s = strings.ReplaceAll(s, "<br/>", "\n")
	s = strings.ReplaceAll(s, "<br />", "\n")
	s = htmlTagPattern.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.Join(strings.Fields(s), " ")
}

func collectJSONIndexedFields(path string, value any, out *[]jsonIndexedField) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			nextPath := key
			if path != "" {
				nextPath = path + "." + key
			}
			collectJSONIndexedFields(nextPath, child, out)
		}
	case []any:
		for i, child := range v {
			nextPath := fmt.Sprintf("[%d]", i)
			if path != "" {
				nextPath = fmt.Sprintf("%s[%d]", path, i)
			}
			collectJSONIndexedFields(nextPath, child, out)
		}
	case string:
		appendJSONIndexedField(path, v, "string", out)
	case float64:
		appendJSONIndexedField(path, strconv.FormatFloat(v, 'f', -1, 64), "number", out)
	case bool:
		appendJSONIndexedField(path, strconv.FormatBool(v), "bool", out)
	case nil:
		appendJSONIndexedField(path, "null", "null", out)
	}
}

func appendJSONIndexedField(path, valueText, valueType string, out *[]jsonIndexedField) {
	if path == "" {
		path = "$"
	}
	*out = append(*out, jsonIndexedField{
		Path:      path,
		ValueText: valueText,
		ValueType: valueType,
	})
}

func indexParseWorkerCount() int {
	n := runtime.NumCPU()
	if n < 2 {
		return 2
	}
	if n > 12 {
		return 12
	}
	return n
}

func loadIndexConfig(fast, ftsBroad bool) indexConfig {
	cfg := indexConfig{
		ParseWorkers:         indexParseWorkerCount(),
		CommitBatchRows:      defaultCommitBatchRows,
		ProgressEvery:        defaultProgressEvery,
		JSONFieldInsertBatch: defaultJSONFieldInsertBatch,
		IndexJSONFields:      true,
		FTSBroad:             ftsBroad,
	}

	if v := os.Getenv("INDEX_PARSE_WORKERS"); v != "" {
		if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && parsed > 0 {
			cfg.ParseWorkers = parsed
		}
	}
	if v := os.Getenv("INDEX_COMMIT_BATCH_ROWS"); v != "" {
		if parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil && parsed > 0 {
			cfg.CommitBatchRows = parsed
		}
	}
	if v := os.Getenv("INDEX_PROGRESS_EVERY"); v != "" {
		if parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil && parsed > 0 {
			cfg.ProgressEvery = parsed
		}
	}
	if v := os.Getenv("INDEX_JSON_INSERT_BATCH"); v != "" {
		if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && parsed > 0 {
			cfg.JSONFieldInsertBatch = parsed
		}
	}
	if v := os.Getenv("INDEX_JSON_FIELDS"); v != "" {
		cfg.IndexJSONFields = parseBool(v)
	}
	if v := os.Getenv("INDEX_FTS_BROAD"); v != "" {
		cfg.FTSBroad = parseBool(v)
	}

	return cfg
}

func parseBool(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y":
		return true
	default:
		return false
	}
}

func requiresLiteralSearch(q string) bool {
	q = strings.TrimSpace(q)
	if q == "" {
		return false
	}
	return strings.ContainsAny(q, ".@:/")
}

func isNoSuchTableErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no such table")
}

func buildFTSExpr(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}

	parts := strings.FieldsFunc(query, func(r rune) bool {
		return !(r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
	})
	if len(parts) == 0 {
		return ""
	}

	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		clean = append(clean, part+"*")
	}
	return strings.Join(clean, " AND ")
}
