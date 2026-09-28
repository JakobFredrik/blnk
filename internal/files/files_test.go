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
	"fmt"
	"math"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collector is a thread-safe StoreFunc target.
type collector struct {
	mu   sync.Mutex
	txns []model.ExternalTransaction
}

func (c *collector) store(_ context.Context, _ string, txn model.ExternalTransaction) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txns = append(c.txns, txn)
	return nil
}

func TestProcessJSON_StreamsAndStampsSource(t *testing.T) {
	body := `[
		{"id":"e1","amount":10.5,"reference":"r1","currency":"USD"},
		{"id":"e2","amount":20,"reference":"r2","currency":"USD"},
		{"id":"e3","amount":30,"reference":"r3","currency":"EUR"}
	]`
	c := &collector{}
	n, err := ProcessJSON(context.Background(), "up_1", "bank_a", strings.NewReader(body), c.store)

	require.NoError(t, err)
	assert.Equal(t, 3, n)
	require.Len(t, c.txns, 3)
	for _, txn := range c.txns {
		assert.Equal(t, "bank_a", txn.Source, "every record must be stamped with the source")
	}
	assert.Equal(t, "e1", c.txns[0].ID)
	assert.Equal(t, 10.5, c.txns[0].Amount)
}

func TestProcessJSON_EmptyArray(t *testing.T) {
	c := &collector{}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(`[]`), c.store)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Empty(t, c.txns)
}

func TestProcessJSON_NonArrayRejected(t *testing.T) {
	c := &collector{}
	_, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(`{"id":"e1"}`), c.store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected a JSON array")
}

func TestProcessJSON_MalformedElementReturnsPartialCount(t *testing.T) {
	// Second element is malformed: the first must have been stored, then the
	// stream aborts with an error and the partial count.
	body := `[{"id":"e1","amount":1},{"id":"e2","amount":}]`
	c := &collector{}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(body), c.store)
	require.Error(t, err)
	assert.Equal(t, 1, n, "the valid leading element is counted before the failure")
	require.Len(t, c.txns, 1)
	assert.Equal(t, "e1", c.txns[0].ID)
}

func TestProcessJSON_StoreErrorPropagates(t *testing.T) {
	body := `[{"id":"e1"},{"id":"e2"}]`
	called := 0
	store := func(_ context.Context, _ string, _ model.ExternalTransaction) error {
		called++
		return fmt.Errorf("store failed")
	}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(body), store)
	require.Error(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, 1, called, "must stop at the first store error, not process the rest")
}

func TestProcessJSON_ContextCancellation(t *testing.T) {
	// Build an array larger than one context-check interval so cancellation
	// is observed mid-stream.
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < ContextCheckInterval*3; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"e%d","amount":1}`, i)
	}
	b.WriteByte(']')

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled

	c := &collector{}
	n, err := ProcessJSON(ctx, "up_1", "src", strings.NewReader(b.String()), c.store)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, n, ContextCheckInterval*3, "cancellation must stop the stream before the end")
}

// TestProcessJSON_LargeInputStreams proves the streaming path handles a large
// array without materializing it; the previous implementation decoded the
// whole slice into memory first.
func TestProcessJSON_LargeInputStreams(t *testing.T) {
	const count = 50000
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"e%d","amount":1,"currency":"USD"}`, i)
	}
	b.WriteByte(']')

	// A counting store keeps the test's own memory flat.
	stored := 0
	store := func(_ context.Context, _ string, _ model.ExternalTransaction) error {
		stored++
		return nil
	}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(b.String()), store)
	require.NoError(t, err)
	assert.Equal(t, count, n)
	assert.Equal(t, count, stored)
}

const csvHeader = "id,amount,currency,reference,description,date\n"

const rfc3339Date = "2026-09-28T10:00:00Z"

func csvRow(id, amount, date string) string {
	return fmt.Sprintf("%s,%s,USD,ref-%s,desc %s,%s\n", id, amount, id, id, date)
}

func (c *collector) sorted() []model.ExternalTransaction {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.ExternalTransaction, len(c.txns))
	copy(out, c.txns)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *collector) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.txns)
}

func processCSVString(t *testing.T, body string) (int, *collector, error) {
	t.Helper()
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "bank_a", strings.NewReader(body), c.store)
	return n, c, err
}

func TestProcessCSV_ParsesRowsAndStampsSource(t *testing.T) {
	body := csvHeader +
		csvRow("e1", "10.50", rfc3339Date) +
		csvRow("e2", "-20", "2026-09-28T12:30:00+02:00") +
		csvRow("e3", "0.01", rfc3339Date)

	n, c, err := processCSVString(t, body)

	require.NoError(t, err)
	assert.Equal(t, 3, n)
	txns := c.sorted()
	require.Len(t, txns, 3)

	assert.Equal(t, model.ExternalTransaction{
		ID:          "e1",
		Amount:      10.5,
		Currency:    "USD",
		Reference:   "ref-e1",
		Description: "desc e1",
		Date:        time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC),
		Source:      "bank_a",
	}, txns[0])
	assert.Equal(t, -20.0, txns[1].Amount)
	assert.True(t, txns[1].Date.Equal(time.Date(2026, 9, 28, 10, 30, 0, 0, time.UTC)), "offset must be preserved as the same instant")
	assert.Equal(t, 0.01, txns[2].Amount)
	for _, txn := range txns {
		assert.Equal(t, "bank_a", txn.Source)
	}
}

func TestProcessCSV_HeaderNormalisationAndColumnOrder(t *testing.T) {
	body := " Date ,AMOUNT,Id,CURRENCY, reference ,Description,extra\n" +
		rfc3339Date + ",42.5,e1,EUR,r1,d1,ignored\n"

	n, c, err := processCSVString(t, body)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.Equal(t, "e1", txns[0].ID)
	assert.Equal(t, 42.5, txns[0].Amount)
	assert.Equal(t, "EUR", txns[0].Currency)
	assert.Equal(t, "r1", txns[0].Reference)
	assert.Equal(t, "d1", txns[0].Description)
	assert.True(t, txns[0].Date.Equal(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)))
}

func TestProcessCSV_TrimsFieldWhitespace(t *testing.T) {
	body := csvHeader + "  e1 , 5.25 , USD , r1 , d1 , " + rfc3339Date + " \n"

	n, c, err := processCSVString(t, body)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.Equal(t, "e1", txns[0].ID)
	assert.Equal(t, 5.25, txns[0].Amount)
	assert.False(t, txns[0].Date.IsZero())
}

func TestProcessCSV_HeaderOnly(t *testing.T) {
	n, c, err := processCSVString(t, csvHeader)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Zero(t, c.len())
}

func TestProcessCSV_EmptyInputRejected(t *testing.T) {
	n, c, err := processCSVString(t, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error reading CSV headers")
	assert.Equal(t, 0, n)
	assert.Zero(t, c.len())
}

func TestCreateColumnMap_MissingRequiredColumn(t *testing.T) {
	tests := []struct {
		name    string
		headers []string
		missing string
	}{
		{"missing id", []string{"amount", "date"}, "ID"},
		{"missing amount", []string{"id", "date"}, "Amount"},
		{"missing date", []string{"id", "amount"}, "Date"},
		{"no headers", []string{}, "ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := createColumnMap(tt.headers)
			require.Error(t, err)
			assert.Equal(t, fmt.Sprintf("required column '%s' not found in CSV", tt.missing), err.Error())
		})
	}
}

func TestProcessCSV_MissingRequiredColumnRejectsFile(t *testing.T) {
	body := "id,currency,reference,description,date\ne1,USD,r1,d1," + rfc3339Date + "\n"

	n, c, err := processCSVString(t, body)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "required column 'Amount' not found")
	assert.Equal(t, 0, n)
	assert.Zero(t, c.len(), "no row may be stored when the header is invalid")
}

// FINDING: header validation only checks ID, Amount and Date, but every row
// also requires Currency, Reference and Description. A file missing one of
// those columns is accepted and then fails on every single row (and hangs
// once enough rows fail, see TestProcessCSV_Hang).
func TestProcessCSV_HeaderValidationIgnoresOtherRequiredFields(t *testing.T) {
	_, err := createColumnMap([]string{"id", "amount", "date"})
	require.NoError(t, err, "FINDING: header without currency/reference/description passes validation")

	body := "id,amount,date\ne1,10," + rfc3339Date + "\n"
	n, c, err := processCSVString(t, body)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "required field 'currency' not found in record")
	assert.Equal(t, 0, n)
	assert.Zero(t, c.len())
}

// FINDING: duplicate headers (e.g. "amount" and "Amount") collapse into one
// map entry, so every row then fails the field-count check with a misleading
// error instead of the header being rejected up front.
func TestProcessCSV_DuplicateHeaderRejectsEveryRow(t *testing.T) {
	headers := []string{"id", "amount", "Amount", "currency", "reference", "description", "date"}
	columnMap, err := createColumnMap(headers)
	require.NoError(t, err)
	assert.Len(t, columnMap, len(headers)-1)
	assert.Equal(t, 2, columnMap["amount"], "last duplicate wins")

	body := strings.Join(headers, ",") + "\ne1,1,2,USD,r1,d1," + rfc3339Date + "\n"
	n, c, err := processCSVString(t, body)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "incorrect number of fields in record")
	assert.Equal(t, 0, n)
	assert.Zero(t, c.len())
}

func TestParseExternalTransaction_RequiredFields(t *testing.T) {
	columnMap, err := createColumnMap(strings.Split(strings.TrimSpace(csvHeader), ","))
	require.NoError(t, err)

	valid := []string{"e1", "1", "USD", "r1", "d1", rfc3339Date}
	for i, field := range []string{"id", "amount", "currency", "reference", "description", "date"} {
		for _, blank := range []string{"", "   "} {
			t.Run(fmt.Sprintf("%s=%q", field, blank), func(t *testing.T) {
				record := append([]string(nil), valid...)
				record[i] = blank
				_, err := parseExternalTransaction(record, columnMap, "src")
				require.Error(t, err)
				assert.Equal(t, fmt.Sprintf("required field '%s' is empty", field), err.Error())
			})
		}
	}

	t.Run("wrong field count", func(t *testing.T) {
		_, err := parseExternalTransaction(valid[:5], columnMap, "src")
		require.Error(t, err)
		assert.Equal(t, "incorrect number of fields in record", err.Error())
	})

	t.Run("missing column", func(t *testing.T) {
		_, err := getRequiredField([]string{"x"}, map[string]int{"id": 0}, "currency")
		require.Error(t, err)
		assert.Equal(t, "required field 'currency' not found in record", err.Error())
	})

	t.Run("index out of range", func(t *testing.T) {
		_, err := getRequiredField([]string{"x"}, map[string]int{"id": 3}, "id")
		require.Error(t, err)
		assert.Equal(t, "required field 'id' not found in record", err.Error())
	})
}

// Each malformed-row test triggers exactly one row error: more errors than
// worker slots deadlock the import (see TestProcessCSV_Hang).
func TestProcessCSV_EmptyRequiredFieldSkipsRow(t *testing.T) {
	body := csvHeader +
		csvRow("e1", "1", rfc3339Date) +
		"e2,1,,r2,d2," + rfc3339Date + "\n" +
		csvRow("e3", "3", rfc3339Date)

	n, c, err := processCSVString(t, body)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "encountered 1 errors")
	assert.Contains(t, err.Error(), "required field 'currency' is empty")
	assert.Equal(t, 2, n, "valid rows are still stored and counted")
	txns := c.sorted()
	require.Len(t, txns, 2)
	assert.Equal(t, "e1", txns[0].ID)
	assert.Equal(t, "e3", txns[1].ID)
}

func TestProcessCSV_MalformedRows(t *testing.T) {
	tests := []struct {
		name    string
		badRow  string
		wantErr string
	}{
		{"too few fields", "e2,1,USD\n", "wrong number of fields"},
		{"too many fields", "e2,1,USD,r2,d2," + rfc3339Date + ",extra\n", "wrong number of fields"},
		{"bare quote", "e2,1,US\"D,r2,d2," + rfc3339Date + "\n", "bare \" in non-quoted-field"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := csvHeader + csvRow("e1", "1", rfc3339Date) + tt.badRow + csvRow("e3", "3", rfc3339Date)

			n, c, err := processCSVString(t, body)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "encountered 1 errors")
			assert.Contains(t, err.Error(), "error reading row")
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Equal(t, 2, n)
			txns := c.sorted()
			require.Len(t, txns, 2)
			assert.Equal(t, "e1", txns[0].ID)
			assert.Equal(t, "e3", txns[1].ID)
		})
	}
}

func TestProcessCSV_QuotedFieldsWithCommas(t *testing.T) {
	body := csvHeader + `e1,1.5,USD,r1,"desc, with comma",` + rfc3339Date + "\n"

	n, c, err := processCSVString(t, body)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.Equal(t, "desc, with comma", txns[0].Description)
}

func TestProcessCSV_StoreErrorPropagates(t *testing.T) {
	store := func(_ context.Context, _ string, _ model.ExternalTransaction) error {
		return fmt.Errorf("store failed")
	}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(csvHeader+csvRow("e1", "1", rfc3339Date)), store)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "store failed")
	assert.Equal(t, 0, n)
}

func TestProcessCSV_PassesUploadIDToStore(t *testing.T) {
	var gotID string
	store := func(_ context.Context, uploadID string, _ model.ExternalTransaction) error {
		gotID = uploadID
		return nil
	}
	_, err := ProcessCSV(context.Background(), "up_42", "src", strings.NewReader(csvHeader+csvRow("e1", "1", rfc3339Date)), store)
	require.NoError(t, err)
	assert.Equal(t, "up_42", gotID)
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

// runWorker feeds records to a single processWorker synchronously, which makes
// batch boundaries deterministic.
func runWorker(t *testing.T, ctx context.Context, count int, store StoreFunc) (int64, []error) {
	t.Helper()
	columnMap, err := createColumnMap(strings.Split(strings.TrimSpace(csvHeader), ","))
	require.NoError(t, err)

	rowChan := make(chan []string, count)
	for i := 0; i < count; i++ {
		rowChan <- []string{fmt.Sprintf("e%d", i), "1", "USD", "r", "d", rfc3339Date}
	}
	close(rowChan)

	errChan := make(chan error, count)
	var total int64
	var mu sync.Mutex
	processWorker(ctx, "up_1", "src", rowChan, columnMap, store, &total, &mu, errChan)
	close(errChan)

	var errs []error
	for e := range errChan {
		errs = append(errs, e)
	}
	return total, errs
}

// FINDING: when the store fails part-way through a batch, the rows stored
// before the failure are persisted but not counted, so the reported total
// is lower than what was actually written.
func TestProcessWorker_UndercountsRowsStoredBeforeStoreError(t *testing.T) {
	stored := 0
	store := func(_ context.Context, _ string, txn model.ExternalTransaction) error {
		if txn.ID == "e1" {
			return fmt.Errorf("store failed")
		}
		stored++
		return nil
	}

	total, errs := runWorker(t, context.Background(), 3, store)

	require.Len(t, errs, 1)
	assert.Equal(t, 1, stored, "e0 was persisted before e1 failed")
	assert.Equal(t, int64(0), total, "FINDING: persisted row e0 is not counted")
}

// FINDING: unlike ProcessJSON, which stops at the first store error, the CSV
// worker keeps storing later batches after a failed one, leaving a partial
// import with gaps.
func TestProcessWorker_ContinuesAfterStoreError(t *testing.T) {
	stored := 0
	store := func(_ context.Context, _ string, txn model.ExternalTransaction) error {
		if txn.ID == "e1" {
			return fmt.Errorf("store failed")
		}
		stored++
		return nil
	}

	total, errs := runWorker(t, context.Background(), 2*DefaultBatchSize, store)

	require.Len(t, errs, 1)
	assert.Equal(t, int64(DefaultBatchSize), total, "second batch is stored and counted")
	assert.Equal(t, DefaultBatchSize+1, stored, "e0 plus the whole second batch were persisted")
}

func TestProcessWorker_FlushesBatchesAndRemainder(t *testing.T) {
	c := &collector{}
	total, errs := runWorker(t, context.Background(), DefaultBatchSize+7, c.store)
	assert.Empty(t, errs)
	assert.Equal(t, int64(DefaultBatchSize+7), total)
	assert.Equal(t, DefaultBatchSize+7, c.len())
}

// FINDING: a cancelled context makes ProcessCSV return a nil error, so the
// caller cannot tell an aborted import from an empty file.
func TestProcessCSV_ContextCancelledBeforeStart(t *testing.T) {
	var b strings.Builder
	b.WriteString(csvHeader)
	for i := 0; i < ContextCheckInterval*3; i++ {
		b.WriteString(csvRow(fmt.Sprintf("e%d", i), "1", rfc3339Date))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &collector{}
	n, err := ProcessCSV(ctx, "up_1", "src", strings.NewReader(b.String()), c.store)

	assert.NoError(t, err, "FINDING: cancellation is not reported as an error")
	assert.Equal(t, 0, n)
	assert.Zero(t, c.len())
}

// FINDING: cancelling mid-import stops at an arbitrary row, silently drops
// rows that were parsed but not yet flushed, and still returns a nil error
// with a partial count.
func TestProcessCSV_ContextCancelledMidStream(t *testing.T) {
	const rows = 10 * DefaultBatchSize
	var b strings.Builder
	b.WriteString(csvHeader)
	for i := 0; i < rows; i++ {
		b.WriteString(csvRow(fmt.Sprintf("e%d", i), "1", rfc3339Date))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &collector{}
	store := func(ctx context.Context, uploadID string, txn model.ExternalTransaction) error {
		cancel()
		return c.store(ctx, uploadID, txn)
	}

	n, err := ProcessCSV(ctx, "up_1", "src", strings.NewReader(b.String()), store)

	assert.NoError(t, err, "FINDING: cancellation is not reported as an error")
	assert.Less(t, n, rows, "import stopped before the end of the file")
	assert.Equal(t, c.len(), n)
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"1234.56", 1234.56},
		{"-10.5", -10.5},
		{"+3", 3},
		{"0", 0},
		{"0.00", 0},
		{".5", 0.5},
		{"100", 100},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, parseFloat(tt.in))
		})
	}
}

// FINDING: any amount strconv.ParseFloat rejects silently becomes 0 instead
// of producing an error, so a record for a real payment is stored as a zero
// amount.
func TestParseFloat_UnparseableAmountsBecomeZero(t *testing.T) {
	for _, in := range []string{
		"1234,56",   // decimal comma
		"1.234,56",  // European thousands + decimal comma
		"1,234.56",  // thousands separator
		"1 234.56",  // space thousands separator
		"1'234.56",  // Swiss thousands separator
		"$10.00",    // currency symbol
		"10.00 USD", // currency suffix
		"(10.00)",   // accounting negative
		"10.00-",    // trailing minus
		"1e400",     // overflow
		"abc",       // garbage
		" 10",       // untrimmed (ProcessCSV trims first)
		"",          // empty
	} {
		t.Run(in, func(t *testing.T) {
			assert.Equal(t, 0.0, parseFloat(in), "FINDING: %q silently parsed as 0", in)
		})
	}
}

// FINDING: strconv.ParseFloat accepts non-monetary syntax, so these values
// are stored as amounts without any error.
func TestParseFloat_AcceptsNonMonetarySyntax(t *testing.T) {
	assert.True(t, math.IsNaN(parseFloat("NaN")), "FINDING: NaN accepted as an amount")
	assert.True(t, math.IsInf(parseFloat("Inf"), 1), "FINDING: +Inf accepted as an amount")
	assert.True(t, math.IsInf(parseFloat("-Infinity"), -1), "FINDING: -Inf accepted as an amount")
	assert.Equal(t, 1000.0, parseFloat("1e3"), "FINDING: exponent notation accepted")
	assert.Equal(t, 16.0, parseFloat("0x1p4"), "FINDING: hex float accepted")
	assert.Equal(t, 1000.0, parseFloat("1_000"), "FINDING: underscore digit separator accepted")
}

// FINDING: amounts are held as float64, so values beyond ~15-17 significant
// digits lose precision and decimal fractions are not exact.
func TestParseFloat_Float64PrecisionLoss(t *testing.T) {
	assert.Equal(t, "12345678901234568.00", fmt.Sprintf("%.2f", parseFloat("12345678901234567.89")),
		"FINDING: large amount rounded by float64")
	assert.NotEqual(t, 0.3, parseFloat("0.1")+parseFloat("0.2"), "FINDING: decimal fractions are inexact")
}

func TestProcessCSV_DecimalCommaAmountStoredAsZero(t *testing.T) {
	body := csvHeader +
		`e1,"1234,56",USD,r1,d1,` + rfc3339Date + "\n"

	n, c, err := processCSVString(t, body)

	// FINDING: a real amount of 1234.56 is imported as 0 with no error.
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.Equal(t, 0.0, txns[0].Amount)
}

func TestProcessCSV_ThousandsSeparatorAmountStoredAsZero(t *testing.T) {
	body := csvHeader +
		`e1,"1,234.56",USD,r1,d1,` + rfc3339Date + "\n"

	n, c, err := processCSVString(t, body)

	// FINDING: a real amount of 1234.56 is imported as 0 with no error.
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.Equal(t, 0.0, txns[0].Amount)
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		in   string
		want time.Time
	}{
		{"2026-09-28T10:00:00Z", time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)},
		{"2026-09-28T12:00:00+02:00", time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)},
		{"2026-09-28T10:00:00.123456789Z", time.Date(2026, 9, 28, 10, 0, 0, 123456789, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := parseTime(tt.in)
			assert.True(t, tt.want.Equal(got), "got %v, want %v", got, tt.want)
		})
	}
}

// FINDING: parseTime only accepts RFC3339; any other date format silently
// becomes the zero time (0001-01-01) instead of producing an error.
func TestParseTime_NonRFC3339BecomesZero(t *testing.T) {
	for _, in := range []string{
		"2026-09-28",
		"2026-09-28 10:00:00",
		"2026-09-28T10:00:00",
		"28/09/2026",
		"09/28/2026",
		"28.09.2026",
		"Sep 28, 2026",
		"1790632800",
		"",
	} {
		t.Run(in, func(t *testing.T) {
			assert.True(t, parseTime(in).IsZero(), "FINDING: %q silently parsed as zero time", in)
		})
	}
}

func TestProcessCSV_DateOnlyStoredAsZeroTime(t *testing.T) {
	n, c, err := processCSVString(t, csvHeader+csvRow("e1", "10", "2026-09-28"))

	// FINDING: a date-only value is imported as 0001-01-01 with no error.
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.True(t, txns[0].Date.IsZero())
	assert.Equal(t, 10.0, txns[0].Amount)
}

func TestDetectByExtension(t *testing.T) {
	assert.Equal(t, "application/json", DetectByExtension("data.json"))
	assert.Equal(t, "application/json", DetectByExtension("DATA.JSON"), "extension match is case-insensitive")
	assert.Equal(t, "", DetectByExtension("data"))
	assert.Equal(t, "", DetectByExtension("data.unknownext"))
	assert.Equal(t, mime.TypeByExtension(".csv"), DetectByExtension("Data.CSV"))
}

func TestLooksLikeCSV(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"header and rows", "id,amount\n1,2\n3,4\n", true},
		{"blank lines skipped", "id,amount\n\n1,2\n", true},
		{"single line", "id,amount", false},
		{"single column", "id\n1\n2\n", false},
		{"inconsistent field count", "id,amount\n1,2,3\n", false},
		{"only first five lines checked", "a,b\n1,2\n1,2\n1,2\n1,2\n1\n", true},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, LooksLikeCSV([]byte(tt.in)))
		})
	}
}

// FINDING: LooksLikeCSV counts raw commas, so valid CSV with a quoted comma
// is not recognised.
func TestLooksLikeCSV_QuotedCommaMiscounted(t *testing.T) {
	assert.False(t, LooksLikeCSV([]byte("id,description\n1,\"a, b\"\n")))
}

func TestAnalyzeTextContent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"csv", "id,amount\n1,2\n", "text/csv"},
		{"json object", `{"id":"e1"}`, "application/json"},
		{"json array", "[{\"id\":\"e1\"},\n{\"id\":\"e2\"}]", "application/json"},
		{"plain text", "hello world", "text/plain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AnalyzeTextContent([]byte(tt.in))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDetectByContent(t *testing.T) {
	t.Run("binary csv-like content falls back to text analysis", func(t *testing.T) {
		got, err := DetectByContent([]byte("id,amount\x01\n1,2\n"))
		require.NoError(t, err)
		assert.Equal(t, "text/csv", got)
	})

	t.Run("other sniffed types pass through", func(t *testing.T) {
		got, err := DetectByContent([]byte("%PDF-1.4\n"))
		require.NoError(t, err)
		assert.Equal(t, "application/pdf", got)
	})

	// FINDING: http.DetectContentType reports text as
	// "text/plain; charset=utf-8", which never matches the "text/plain" case,
	// so CSV and JSON content is never recognised by sniffing.
	for name, body := range map[string]string{
		"csv":  csvHeader + csvRow("e1", "1", rfc3339Date),
		"json": `[{"id":"e1","amount":1}]`,
	} {
		t.Run(name+" text is not recognised", func(t *testing.T) {
			got, err := DetectByContent([]byte(body))
			require.NoError(t, err)
			assert.Equal(t, "text/plain; charset=utf-8", got, "FINDING: %s content not detected", name)
		})
	}
}

func TestDetectFileType(t *testing.T) {
	t.Run("extension wins over content", func(t *testing.T) {
		got, err := DetectFileType([]byte(csvHeader), "data.json")
		require.NoError(t, err)
		assert.Equal(t, "application/json", got)
	})

	t.Run("no extension falls back to content", func(t *testing.T) {
		got, err := DetectFileType([]byte("%PDF-1.4\n"), "upload")
		require.NoError(t, err)
		assert.Equal(t, "application/pdf", got)
	})
}

func TestParseAndStoreData_UnsupportedType(t *testing.T) {
	c := &collector{}
	n, err := parseAndStoreData(context.Background(), "up_1", "src", strings.NewReader("x"), "text/plain", c.store)
	require.EqualError(t, err, "unsupported file type: text/plain")
	assert.Equal(t, 0, n)
}

func uploadTempFiles(t *testing.T, filename string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "blnk_uploads", filepath.Base(filename)+"_*"))
	require.NoError(t, err)
	return matches
}

func uniqueFilename(t *testing.T, ext string) string {
	return fmt.Sprintf("%s_%d%s", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano(), ext)
}

func TestUploadExternalData_JSON(t *testing.T) {
	filename := uniqueFilename(t, ".json")
	c := &collector{}

	id, n, err := UploadExternalData(context.Background(), "bank_a", strings.NewReader(`[{"id":"e1","amount":5}]`), filename, c.store)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(id, "upload_"), "got upload id %q", id)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.Equal(t, "bank_a", txns[0].Source)
	assert.Empty(t, uploadTempFiles(t, filename), "temporary file must be removed")
}

func TestUploadExternalData_CSV(t *testing.T) {
	if !strings.HasPrefix(mime.TypeByExtension(".csv"), "text/csv") {
		t.Skip("system mime table does not map .csv to text/csv")
	}
	filename := uniqueFilename(t, ".csv")
	c := &collector{}

	id, n, err := UploadExternalData(context.Background(), "bank_a", strings.NewReader(csvHeader+csvRow("e1", "7.5", rfc3339Date)), filename, c.store)

	require.NoError(t, err)
	assert.NotEmpty(t, id)
	assert.Equal(t, 1, n)
	txns := c.sorted()
	require.Len(t, txns, 1)
	assert.Equal(t, 7.5, txns[0].Amount)
	assert.Empty(t, uploadTempFiles(t, filename))
}

// FINDING: an upload without a known extension is rejected even when its
// content is valid CSV or JSON. Files over 512 bytes are sniffed as
// "text/plain; charset=utf-8" (see TestDetectByContent). Smaller files are
// misdetected as "text/plain" because detectFileTypeFromTempFile passes the
// whole zero-padded 512-byte buffer, not just the bytes read, and the NUL
// padding defeats both LooksLikeCSV and json.Valid.
func TestUploadExternalData_NoExtensionRejected(t *testing.T) {
	var largeCSV strings.Builder
	largeCSV.WriteString(csvHeader)
	for i := 0; i < 20; i++ {
		largeCSV.WriteString(csvRow(fmt.Sprintf("e%d", i), "1", rfc3339Date))
	}
	require.Greater(t, largeCSV.Len(), 512)

	tests := []struct {
		name     string
		body     string
		detected string
	}{
		{"small csv", csvHeader + csvRow("e1", "1", rfc3339Date), "text/plain"},
		{"small json", `[{"id":"e1","amount":1}]`, "text/plain"},
		{"large csv", largeCSV.String(), "text/plain; charset=utf-8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := uniqueFilename(t, "")
			c := &collector{}

			id, n, err := UploadExternalData(context.Background(), "src", strings.NewReader(tt.body), filename, c.store)

			require.EqualError(t, err, "unsupported file type: "+tt.detected, "FINDING: valid %s upload rejected", tt.name)
			assert.Empty(t, id)
			assert.Equal(t, 0, n)
			assert.Zero(t, c.len())
			assert.Empty(t, uploadTempFiles(t, filename))
		})
	}
}

func TestUploadExternalData_UnsupportedExtension(t *testing.T) {
	filename := uniqueFilename(t, ".pdf")
	_, _, err := UploadExternalData(context.Background(), "src", strings.NewReader("%PDF-1.4\n"), filename, (&collector{}).store)
	require.EqualError(t, err, "unsupported file type: application/pdf")
}

func TestUploadExternalData_ParseErrorPropagates(t *testing.T) {
	filename := uniqueFilename(t, ".json")
	id, n, err := UploadExternalData(context.Background(), "src", strings.NewReader(`{"id":"e1"}`), filename, (&collector{}).store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected a JSON array")
	assert.Empty(t, id)
	assert.Equal(t, 0, n)
	assert.Empty(t, uploadTempFiles(t, filename))
}

// FINDING: errChan is buffered to the worker count (at most 4) and only drained
// after all workers exit. Once more rows fail than that, the reader and
// workers block on errChan forever and ProcessCSV never returns.
//
// Skipped by default; run with:
//
//	RUN_HANG_TEST=1 go test -run TestProcessCSV_Hang -v ./internal/files/...
func TestProcessCSV_Hang(t *testing.T) {
	if os.Getenv("RUN_HANG_TEST") != "1" {
		t.Skip("set RUN_HANG_TEST=1 to run; ProcessCSV deadlocks on these inputs")
	}

	const timeout = 5 * time.Second
	okStore := func(context.Context, string, model.ExternalTransaction) error { return nil }
	failStore := func(context.Context, string, model.ExternalTransaction) error { return fmt.Errorf("store failed") }

	rows := func(header string, count int, row func(i int) string) string {
		var b strings.Builder
		b.WriteString(header)
		for i := 0; i < count; i++ {
			b.WriteString(row(i))
		}
		return b.String()
	}

	tests := []struct {
		name  string
		body  string
		store StoreFunc
	}{
		{
			name: "rows with an empty required field",
			body: rows(csvHeader, 10, func(i int) string {
				return fmt.Sprintf("e%d,1,USD,,d,%s\n", i, rfc3339Date)
			}),
			store: okStore,
		},
		{
			name: "rows with the wrong field count",
			body: rows(csvHeader, 10, func(i int) string {
				return fmt.Sprintf("e%d,1\n", i)
			}),
			store: okStore,
		},
		{
			name: "header without currency, reference and description",
			body: rows("id,amount,date\n", 10, func(i int) string {
				return fmt.Sprintf("e%d,1,%s\n", i, rfc3339Date)
			}),
			store: okStore,
		},
		{
			name: "store failing across many batches",
			body: rows(csvHeader, 10*DefaultBatchSize, func(i int) string {
				return csvRow(fmt.Sprintf("e%d", i), "1", rfc3339Date)
			}),
			store: failStore,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(tt.body), tt.store)
			}()
			select {
			case <-done:
			case <-time.After(timeout):
				t.Errorf("FINDING: ProcessCSV did not return within %s (deadlock on errChan)", timeout)
			}
		})
	}
}
