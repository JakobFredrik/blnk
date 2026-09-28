/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package files

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFloat(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want float64
	}{
		{"integer", "100", 100},
		{"decimal point", "1234.56", 1234.56},
		{"negative", "-20.5", -20.5},
		{"explicit plus", "+3", 3},
		{"zero", "0", 0},
		{"leading dot", ".5", 0.5},
		{"trailing dot", "5.", 5},
		{"many decimals", "0.000001", 0.000001},

		// FINDING: each of these is a plausible bank-export amount that is
		// silently converted to 0 instead of being rejected.
		{"FINDING decimal comma", "1234,56", 0},
		{"FINDING thousands comma", "1,234.56", 0},
		{"FINDING european thousands dot", "1.234,56", 0},
		{"FINDING thousands space", "1 234.56", 0},
		{"FINDING apostrophe thousands", "1'234.56", 0},
		{"FINDING currency symbol", "$5.00", 0},
		{"FINDING currency code", "5.00 USD", 0},
		{"FINDING accounting negative", "(5.00)", 0},
		{"FINDING trailing minus", "5.00-", 0},
		{"FINDING text", "abc", 0},

		// FINDING: these are accepted as numbers although no bank export
		// would mean them that way.
		{"FINDING scientific notation", "1e3", 1000},
		{"FINDING underscore separator", "1_000", 1000},
		{"FINDING hex float", "0x1p4", 16},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseFloat(tc.in))
		})
	}
}

// FINDING: NaN and infinities parse successfully and would be stored as the
// transaction amount.
func TestParseFloat_FINDING_NaNAndInfAccepted(t *testing.T) {
	assert.True(t, math.IsNaN(parseFloat("NaN")))
	assert.True(t, math.IsInf(parseFloat("Inf"), 1))
	assert.True(t, math.IsInf(parseFloat("-Infinity"), -1))
}

// FINDING: amounts are float64, so values beyond ~15-17 significant digits
// lose precision and common decimal fractions are not exact.
func TestParseFloat_FINDING_Float64PrecisionLoss(t *testing.T) {
	assert.Equal(t, 12345678901234568.0, parseFloat("12345678901234567.89"))
	assert.NotEqual(t, 0.3, parseFloat("0.1")+parseFloat("0.2"))
}

func TestParseTime(t *testing.T) {
	plus2 := time.FixedZone("", 2*60*60)
	cases := []struct {
		name string
		in   string
		want time.Time
	}{
		{"RFC3339 UTC", "2026-09-28T10:00:00Z", time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)},
		{"RFC3339 offset", "2026-09-28T10:00:00+02:00", time.Date(2026, 9, 28, 10, 0, 0, 0, plus2)},
		{"RFC3339 fractional seconds", "2026-09-28T10:00:00.123Z", time.Date(2026, 9, 28, 10, 0, 0, 123000000, time.UTC)},

		// FINDING: each of these is silently converted to the zero time
		// (0001-01-01T00:00:00Z) instead of being rejected.
		{"FINDING date only", "2026-09-28", time.Time{}},
		{"FINDING space separator", "2026-09-28 10:00:00", time.Time{}},
		{"FINDING no timezone", "2026-09-28T10:00:00", time.Time{}},
		{"FINDING day first", "28/09/2026", time.Time{}},
		{"FINDING month first", "09/28/2026", time.Time{}},
		{"FINDING compact", "20260928", time.Time{}},
		{"FINDING unix timestamp", "1790622000", time.Time{}},
		{"FINDING text", "yesterday", time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTime(tc.in)
			assert.True(t, tc.want.Equal(got), "parseTime(%q) = %v, want %v", tc.in, got, tc.want)
			assert.Equal(t, tc.want.IsZero(), got.IsZero())
		})
	}
}

func TestCreateColumnMap(t *testing.T) {
	m, err := createColumnMap([]string{" ID ", "amount", "DATE", "Extra Col"})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"id": 0, "amount": 1, "date": 2, "extra col": 3}, m)

	_, err = createColumnMap(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required column 'ID' not found")
}

func TestGetRequiredField(t *testing.T) {
	columnMap := map[string]int{"id": 0, "amount": 1, "ghost": 5}
	record := []string{"  e1  ", "   "}

	v, err := getRequiredField(record, columnMap, "id")
	require.NoError(t, err)
	assert.Equal(t, "e1", v)

	_, err = getRequiredField(record, columnMap, "amount")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required field 'amount' is empty")

	_, err = getRequiredField(record, columnMap, "currency")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required field 'currency' not found")

	_, err = getRequiredField(record, columnMap, "ghost")
	require.Error(t, err, "index beyond the record is treated as missing")
	assert.Contains(t, err.Error(), "required field 'ghost' not found")
}

func TestParseExternalTransaction_FieldCountMismatch(t *testing.T) {
	columnMap, err := createColumnMap(strings.Split(csvHeader, ","))
	require.NoError(t, err)

	_, err = parseExternalTransaction([]string{"e1", "1"}, columnMap, "src")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incorrect number of fields in record")
}

func TestStoreBatch_StopsAtFirstError(t *testing.T) {
	batch := []model.ExternalTransaction{{ID: "e1"}, {ID: "e2"}, {ID: "e3"}}
	var called []string
	store := func(_ context.Context, _ string, txn model.ExternalTransaction) error {
		called = append(called, txn.ID)
		if txn.ID == "e2" {
			return fmt.Errorf("store failed")
		}
		return nil
	}
	err := storeBatch(context.Background(), "up_1", batch, store)
	require.EqualError(t, err, "store failed")
	assert.Equal(t, []string{"e1", "e2"}, called)
}

func TestDetectByExtension(t *testing.T) {
	assert.True(t, strings.HasPrefix(DetectByExtension("bank.csv"), "text/csv"))
	assert.True(t, strings.HasPrefix(DetectByExtension("BANK.CSV"), "text/csv"), "extension match is case-insensitive")
	assert.True(t, strings.HasPrefix(DetectByExtension("dir/bank.export.csv"), "text/csv"))
	assert.Equal(t, "application/json", DetectByExtension("bank.json"))
	assert.True(t, strings.HasPrefix(DetectByExtension("bank.txt"), "text/plain"))
	assert.Empty(t, DetectByExtension("bank"))
	assert.Empty(t, DetectByExtension(""))
}

func TestLooksLikeCSV(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"consistent columns", "a,b,c\n1,2,3\n4,5,6", true},
		{"CRLF line endings", "a,b\r\n1,2\r\n", true},
		{"blank lines skipped", "a,b\n\n1,2\n", true},
		{"only first five lines checked", "a,b\n1,2\n1,2\n1,2\n1,2\n1,2,3,4", true},
		{"single line", "a,b,c", false},
		{"single column", "a\n1\n2", false},
		{"inconsistent columns", "a,b,c\n1,2\n", false},
		{"empty", "", false},
		// Commas inside quoted fields are counted as separators.
		{"quoted comma", "a,b\n\"1,5\",2\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, LooksLikeCSV([]byte(tc.in)))
		})
	}
}

func TestAnalyzeTextContent(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"csv", "id,amount\n1,2\n", "text/csv"},
		{"json array", `[{"id":"e1"}]`, "application/json"},
		{"json object", `{"id":"e1"}`, "application/json"},
		{"plain text", "hello world", "text/plain"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AnalyzeTextContent([]byte(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDetectByContent(t *testing.T) {
	// Binary-looking data (control bytes) is routed through AnalyzeTextContent.
	got, err := DetectByContent([]byte("id,amount\n1,2\x01"))
	require.NoError(t, err)
	assert.Equal(t, "text/csv", got)

	got, err = DetectByContent([]byte("\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR"))
	require.NoError(t, err)
	assert.Equal(t, "image/png", got)
}

// FINDING: http.DetectContentType reports ordinary text as
// "text/plain; charset=utf-8", which DetectByContent does not match, so plain
// CSV and JSON content is never recognised by content sniffing.
func TestDetectByContent_FINDING_PlainTextCSVAndJSONNotRecognised(t *testing.T) {
	for name, body := range map[string]string{
		"csv":  csvBody(csvHeader, "e1,1,USD,r,d,2026-09-28T10:00:00Z"),
		"json": `[{"id":"e1","amount":1}]`,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := DetectByContent([]byte(body))
			require.NoError(t, err)
			assert.Equal(t, "text/plain; charset=utf-8", got)
		})
	}
}

func TestDetectFileType_ExtensionTakesPrecedence(t *testing.T) {
	got, err := DetectFileType([]byte(`[{"id":"e1"}]`), "data.csv")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "text/csv"), "extension wins over JSON content")

	got, err = DetectFileType([]byte("id,amount\n1,2\x01"), "data")
	require.NoError(t, err)
	assert.Equal(t, "text/csv", got, "falls back to content without an extension")
}

func TestParseAndStoreData_Dispatch(t *testing.T) {
	csvData := csvBody(csvHeader, "e1,1,USD,r,d,2026-09-28T10:00:00Z")
	jsonData := `[{"id":"j1","amount":1}]`
	cases := []struct {
		fileType string
		body     string
		wantID   string
	}{
		{"text/csv", csvData, "e1"},
		{"text/csv; charset=utf-8", csvData, "e1"},
		{"application/json", jsonData, "j1"},
	}
	for _, tc := range cases {
		t.Run(tc.fileType, func(t *testing.T) {
			c := &collector{}
			n, err := parseAndStoreData(context.Background(), "up_1", "src", strings.NewReader(tc.body), tc.fileType, c.store)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			assert.Equal(t, []string{tc.wantID}, c.ids())
		})
	}

	c := &collector{}
	_, err := parseAndStoreData(context.Background(), "up_1", "src", strings.NewReader(csvData), "text/plain", c.store)
	require.EqualError(t, err, "unsupported file type: text/plain")
	assert.Empty(t, c.txns)
}

// uploadTempDir points os.TempDir at a per-test directory and returns the
// directory UploadExternalData writes its temporary files to.
func uploadTempDir(t *testing.T) string {
	t.Helper()
	t.Setenv("TMPDIR", t.TempDir())
	return filepath.Join(os.TempDir(), "blnk_uploads")
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestUploadExternalData_CSV(t *testing.T) {
	dir := uploadTempDir(t)
	body := csvBody(csvHeader,
		"e1,10.5,USD,r1,d,2026-09-28T10:00:00Z",
		"e2,20,USD,r2,d,2026-09-28T11:00:00Z",
	)

	var seenUploadIDs []string
	c := &collector{}
	store := func(ctx context.Context, uploadID string, txn model.ExternalTransaction) error {
		c.mu.Lock()
		seenUploadIDs = append(seenUploadIDs, uploadID)
		c.mu.Unlock()
		return c.store(ctx, uploadID, txn)
	}

	uploadID, n, err := UploadExternalData(context.Background(), "bank_a", strings.NewReader(body), "statement.csv", store)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(uploadID, "upload_"))
	assert.Equal(t, 2, n)
	assert.Equal(t, []string{"e1", "e2"}, c.ids())
	assert.Equal(t, []string{uploadID, uploadID}, seenUploadIDs)
	for _, txn := range c.txns {
		assert.Equal(t, "bank_a", txn.Source)
	}
	assert.Empty(t, dirEntries(t, dir), "temporary file is removed")
}

func TestUploadExternalData_JSON(t *testing.T) {
	dir := uploadTempDir(t)
	c := &collector{}
	uploadID, n, err := UploadExternalData(context.Background(), "bank_a",
		strings.NewReader(`[{"id":"j1","amount":1},{"id":"j2","amount":2}]`), "statement.json", c.store)
	require.NoError(t, err)
	assert.NotEmpty(t, uploadID)
	assert.Equal(t, 2, n)
	assert.Equal(t, []string{"j1", "j2"}, c.ids())
	assert.Empty(t, dirEntries(t, dir))
}

func TestUploadExternalData_UnsupportedExtension(t *testing.T) {
	dir := uploadTempDir(t)
	c := &collector{}
	body := csvBody(csvHeader, "e1,1,USD,r,d,2026-09-28T10:00:00Z")
	uploadID, n, err := UploadExternalData(context.Background(), "src", strings.NewReader(body), "statement.txt", c.store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported file type: text/plain")
	assert.Empty(t, uploadID)
	assert.Equal(t, 0, n)
	assert.Empty(t, c.txns)
	assert.Empty(t, dirEntries(t, dir))
}

// FINDING: a file without a recognised extension is always rejected. The
// 512-byte sniffing buffer is zero-padded for short files (so neither the CSV
// heuristic nor json.Valid match), and longer text is reported as
// "text/plain; charset=utf-8", which is also unsupported.
func TestUploadExternalData_FINDING_ExtensionlessFilesRejected(t *testing.T) {
	short := csvBody(csvHeader, "e1,1,USD,r,d,2026-09-28T10:00:00Z")
	var long strings.Builder
	long.WriteString(csvHeader + "\n")
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&long, "e%d,1,USD,r,d,2026-09-28T10:00:00Z\n", i)
	}

	cases := []struct {
		name     string
		body     string
		wantType string
	}{
		{"short csv", short, "unsupported file type: text/plain"},
		{"short json", `[{"id":"j1","amount":1}]`, "unsupported file type: text/plain"},
		{"long csv", long.String(), "unsupported file type: text/plain; charset=utf-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uploadTempDir(t)
			c := &collector{}
			_, _, err := UploadExternalData(context.Background(), "src", strings.NewReader(tc.body), "statement", c.store)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantType)
			assert.Empty(t, c.txns)
		})
	}
}

// FINDING: on any processing error UploadExternalData returns an empty upload
// ID and a zero count, even though records before the failure were already
// stored under that upload ID.
func TestUploadExternalData_FINDING_PartialFailureHidesStoredRecords(t *testing.T) {
	uploadTempDir(t)
	var storedUnder []string
	store := func(_ context.Context, uploadID string, txn model.ExternalTransaction) error {
		if txn.ID == "j2" {
			return fmt.Errorf("store failed")
		}
		storedUnder = append(storedUnder, uploadID)
		return nil
	}
	uploadID, n, err := UploadExternalData(context.Background(), "src",
		strings.NewReader(`[{"id":"j1"},{"id":"j2"},{"id":"j3"}]`), "statement.json", store)
	require.EqualError(t, err, "store failed")
	assert.Empty(t, uploadID, "upload ID is not returned")
	assert.Equal(t, 0, n, "count is not returned")
	require.Len(t, storedUnder, 1, "but j1 was persisted")
	assert.True(t, strings.HasPrefix(storedUnder[0], "upload_"))
}

// FINDING: if reading the upload fails, the temporary file is left behind in
// the blnk_uploads directory.
func TestUploadExternalData_FINDING_ReadErrorLeaksTempFile(t *testing.T) {
	dir := uploadTempDir(t)
	c := &collector{}
	_, _, err := UploadExternalData(context.Background(), "src",
		iotest.ErrReader(errors.New("connection reset")), "statement.csv", c.store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error copying upload data")
	assert.Len(t, dirEntries(t, dir), 1, "temporary file is not cleaned up")
}

func TestCleanupTempFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cleanup_")
	require.NoError(t, err)
	name := f.Name()

	cleanupTempFile(f)
	_, err = os.Stat(name)
	assert.True(t, errors.Is(err, os.ErrNotExist))

	assert.NotPanics(t, func() { cleanupTempFile(nil) })
	// Second cleanup of the same file only logs the close/remove errors.
	assert.NotPanics(t, func() { cleanupTempFile(f) })
}
