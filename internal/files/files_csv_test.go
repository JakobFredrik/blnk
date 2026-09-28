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
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const csvHeader = "ID,Amount,Currency,Reference,Description,Date"

func csvBody(header string, rows ...string) string {
	return header + "\n" + strings.Join(rows, "\n") + "\n"
}

// sortedByID returns the collected transactions ordered by ID; CSV rows are
// processed by parallel workers, so storage order is not deterministic.
func (c *collector) sortedByID() []model.ExternalTransaction {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.ExternalTransaction, len(c.txns))
	copy(out, c.txns)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *collector) ids() []string {
	var ids []string
	for _, txn := range c.sortedByID() {
		ids = append(ids, txn.ID)
	}
	return ids
}

func TestProcessCSV_ParsesRowsAndStampsSource(t *testing.T) {
	body := csvBody(csvHeader,
		"e1,10.50,USD,ref-1,Card payment,2026-09-28T10:00:00Z",
		"e2,-20,EUR,ref-2,Refund,2026-09-29T08:30:00+02:00",
		"e3,0.01,GBP,ref-3,Fee,2026-09-30T00:00:00Z",
	)
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "bank_a", strings.NewReader(body), c.store)

	require.NoError(t, err)
	assert.Equal(t, 3, n)
	txns := c.sortedByID()
	require.Len(t, txns, 3)

	assert.Equal(t, model.ExternalTransaction{
		ID:          "e1",
		Amount:      10.50,
		Currency:    "USD",
		Reference:   "ref-1",
		Description: "Card payment",
		Date:        time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC),
		Source:      "bank_a",
	}, txns[0])
	assert.Equal(t, -20.0, txns[1].Amount)
	assert.True(t, txns[1].Date.Equal(time.Date(2026, 9, 29, 6, 30, 0, 0, time.UTC)))
	for _, txn := range txns {
		assert.Equal(t, "bank_a", txn.Source, "every record must be stamped with the source")
	}
}

func TestProcessCSV_HeaderMappingIsCaseAndWhitespaceInsensitive(t *testing.T) {
	// Columns reordered, mixed case, padded, plus an unrelated extra column.
	header := " date ,NOTES,Description,  AMOUNT,reference,Currency,id"
	body := csvBody(header, "2026-09-28T10:00:00Z,ignored,Wire,99.99,ref-1,USD,e1")
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.Len(t, c.txns, 1)
	txn := c.txns[0]
	assert.Equal(t, "e1", txn.ID)
	assert.Equal(t, 99.99, txn.Amount)
	assert.Equal(t, "USD", txn.Currency)
	assert.Equal(t, "ref-1", txn.Reference)
	assert.Equal(t, "Wire", txn.Description)
	assert.Equal(t, time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC), txn.Date)
}

func TestProcessCSV_FieldValuesAreTrimmed(t *testing.T) {
	body := csvBody(csvHeader, "  e1 , 5.25 , USD , ref-1 , Card , 2026-09-28T10:00:00Z ")
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.Len(t, c.txns, 1)
	assert.Equal(t, "e1", c.txns[0].ID)
	assert.Equal(t, 5.25, c.txns[0].Amount)
	assert.Equal(t, time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC), c.txns[0].Date)
}

func TestProcessCSV_MissingRequiredColumn(t *testing.T) {
	cases := []struct {
		missing string
		header  string
	}{
		{"ID", "Amount,Currency,Reference,Description,Date"},
		{"Amount", "ID,Currency,Reference,Description,Date"},
		{"Date", "ID,Amount,Currency,Reference,Description"},
	}
	for _, tc := range cases {
		t.Run(tc.missing, func(t *testing.T) {
			c := &collector{}
			n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(tc.header+"\n"), c.store)
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("required column '%s' not found", tc.missing))
			assert.Equal(t, 0, n)
			assert.Empty(t, c.txns)
		})
	}
}

// FINDING: the header check only requires ID, Amount and Date, but every row
// also requires non-empty Currency, Reference and Description. A file without
// one of those columns passes header validation and then fails every row.
// Only one data row is used here: with more rows than the error channel can
// buffer the import hangs (see TestProcessCSV_ManyBadRowsHangs).
func TestProcessCSV_FINDING_OptionalColumnsAreRequiredPerRow(t *testing.T) {
	for _, missing := range []string{"Currency", "Reference", "Description"} {
		t.Run(missing, func(t *testing.T) {
			var cols []string
			for _, col := range strings.Split(csvHeader, ",") {
				if col != missing {
					cols = append(cols, col)
				}
			}
			header := strings.Join(cols, ",")
			row := strings.Repeat("x,", len(cols)-1) + "x"

			_, err := createColumnMap(strings.Split(header, ","))
			require.NoError(t, err, "header validation accepts the file")

			c := &collector{}
			n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(csvBody(header, row)), c.store)
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("required field '%s' not found", strings.ToLower(missing)))
			assert.Equal(t, 0, n)
			assert.Empty(t, c.txns)
		})
	}
}

// FINDING: a UTF-8 byte order mark (as written by Excel "CSV UTF-8" exports)
// is not stripped from the first header, so a valid file is rejected.
func TestProcessCSV_FINDING_ByteOrderMarkBreaksFirstHeader(t *testing.T) {
	body := "\ufeff" + csvBody(csvHeader, "e1,1,USD,r,d,2026-09-28T10:00:00Z")
	c := &collector{}
	_, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required column 'ID' not found")
	assert.Empty(t, c.txns)
}

// FINDING: duplicate header names collapse into one column-map entry, so every
// row then has more fields than the map and is rejected.
func TestProcessCSV_FINDING_DuplicateHeaderRejectsEveryRow(t *testing.T) {
	header := csvHeader + ",Notes,notes"
	body := csvBody(header, "e1,1,USD,r,d,2026-09-28T10:00:00Z,a,b")
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incorrect number of fields in record")
	assert.Equal(t, 0, n)
	assert.Empty(t, c.txns)
}

func TestProcessCSV_EmptyInput(t *testing.T) {
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(""), c.store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error reading CSV headers")
	assert.Equal(t, 0, n)
}

func TestProcessCSV_HeaderOnly(t *testing.T) {
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(csvHeader+"\n"), c.store)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Empty(t, c.txns)
}

func TestProcessCSV_EmptyRequiredFieldRejectsOnlyThatRow(t *testing.T) {
	valid := map[string]string{
		"id": "bad", "amount": "1", "currency": "USD",
		"reference": "r", "description": "d", "date": "2026-09-28T10:00:00Z",
	}
	order := []string{"id", "amount", "currency", "reference", "description", "date"}

	for _, field := range order {
		for name, blank := range map[string]string{"empty": "", "whitespace": "   "} {
			t.Run(field+"/"+name, func(t *testing.T) {
				var fields []string
				for _, col := range order {
					if col == field {
						fields = append(fields, blank)
					} else {
						fields = append(fields, valid[col])
					}
				}
				body := csvBody(csvHeader,
					"ok1,1,USD,r,d,2026-09-28T10:00:00Z",
					strings.Join(fields, ","),
					"ok2,2,USD,r,d,2026-09-28T10:00:00Z",
				)
				c := &collector{}
				n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "encountered 1 errors")
				assert.Contains(t, err.Error(), fmt.Sprintf("required field '%s' is empty", field))
				assert.Equal(t, 2, n, "valid rows are still stored")
				assert.Equal(t, []string{"ok1", "ok2"}, c.ids())
			})
		}
	}
}

func TestProcessCSV_MalformedRows(t *testing.T) {
	cases := []struct {
		name    string
		row     string
		wantErr string
	}{
		{"too many fields", "bad,1,USD,r,d,2026-09-28T10:00:00Z,extra", "wrong number of fields"},
		{"too few fields", "bad,1,USD,r,d", "wrong number of fields"},
		{"bare quote", `bad,1,USD,r"x,d,2026-09-28T10:00:00Z`, `bare " in non-quoted-field`},
		// An unquoted decimal comma splits the amount into two fields.
		{"unquoted decimal comma", "bad,1234,56,USD,r,d,2026-09-28T10:00:00Z", "wrong number of fields"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := csvBody(csvHeader,
				"ok1,1,USD,r,d,2026-09-28T10:00:00Z",
				tc.row,
				"ok2,2,USD,r,d,2026-09-28T10:00:00Z",
			)
			c := &collector{}
			n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "error reading row")
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Equal(t, 2, n, "rows around the malformed one are still stored")
			assert.Equal(t, []string{"ok1", "ok2"}, c.ids())
		})
	}
}

// FINDING: amounts that strconv.ParseFloat cannot parse are stored as 0 with
// no error, so a quoted decimal-comma or thousands-separated amount silently
// becomes a zero-value transaction.
func TestProcessCSV_FINDING_UnparseableAmountStoredAsZero(t *testing.T) {
	body := csvBody(csvHeader,
		`e1,"1234,56",EUR,r,d,2026-09-28T10:00:00Z`,
		`e2,"1,234.56",USD,r,d,2026-09-28T10:00:00Z`,
		`e3,1.234.56,EUR,r,d,2026-09-28T10:00:00Z`,
		`e4,$5.00,USD,r,d,2026-09-28T10:00:00Z`,
	)
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)

	require.NoError(t, err, "no error is reported for unparseable amounts")
	assert.Equal(t, 4, n)
	for _, txn := range c.sortedByID() {
		assert.Zero(t, txn.Amount, "amount for %s silently became 0", txn.ID)
	}
}

// FINDING: dates that are not RFC3339 are stored as the zero time
// (0001-01-01) with no error, including plain date-only values.
func TestProcessCSV_FINDING_NonRFC3339DateStoredAsZeroTime(t *testing.T) {
	body := csvBody(csvHeader,
		"e1,1,USD,r,d,2026-09-28",
		"e2,1,USD,r,d,2026-09-28 10:00:00",
		"e3,1,USD,r,d,28/09/2026",
	)
	c := &collector{}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), c.store)

	require.NoError(t, err, "no error is reported for unparseable dates")
	assert.Equal(t, 3, n)
	for _, txn := range c.sortedByID() {
		assert.True(t, txn.Date.IsZero(), "date for %s silently became the zero time", txn.ID)
	}
}

func TestProcessCSV_StoreErrorPropagates(t *testing.T) {
	body := csvBody(csvHeader,
		"e1,1,USD,r,d,2026-09-28T10:00:00Z",
		"e2,2,USD,r,d,2026-09-28T10:00:00Z",
		"e3,3,USD,r,d,2026-09-28T10:00:00Z",
	)
	store := func(_ context.Context, _ string, _ model.ExternalTransaction) error {
		return fmt.Errorf("store failed")
	}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(body), store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store failed")
	assert.Equal(t, 0, n)
}

func TestProcessCSV_PassesUploadIDToStore(t *testing.T) {
	body := csvBody(csvHeader, "e1,1,USD,r,d,2026-09-28T10:00:00Z")
	var mu sync.Mutex
	var got []string
	store := func(_ context.Context, uploadID string, _ model.ExternalTransaction) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, uploadID)
		return nil
	}
	_, err := ProcessCSV(context.Background(), "up_42", "src", strings.NewReader(body), store)
	require.NoError(t, err)
	assert.Equal(t, []string{"up_42"}, got)
}

// FINDING: a store failure part-way through a batch leaves the earlier rows of
// that batch persisted but excluded from the reported count.
func TestProcessWorker_FINDING_PartialBatchStoreFailureUndercounts(t *testing.T) {
	columnMap, err := createColumnMap(strings.Split(csvHeader, ","))
	require.NoError(t, err)

	rowChan := make(chan []string, 3)
	for _, id := range []string{"e1", "e2", "e3"} {
		rowChan <- []string{id, "1", "USD", "r", "d", "2026-09-28T10:00:00Z"}
	}
	close(rowChan)

	var persisted []string
	store := func(_ context.Context, _ string, txn model.ExternalTransaction) error {
		if txn.ID == "e2" {
			return fmt.Errorf("store failed")
		}
		persisted = append(persisted, txn.ID)
		return nil
	}

	var total int64
	var mu sync.Mutex
	errChan := make(chan error, 1)
	processWorker(context.Background(), "up_1", "src", rowChan, columnMap, store, &total, &mu, errChan)
	close(errChan)

	require.Len(t, errChan, 1)
	assert.Equal(t, []string{"e1"}, persisted, "e1 was written before the failure")
	assert.Equal(t, int64(0), total, "but it is not included in the count")
}

// FINDING: a canceled CSV import returns a nil error with a partial (here
// zero) count, so callers cannot tell it apart from a successful import of an
// empty file. ProcessJSON returns context.Canceled in the same situation.
func TestProcessCSV_FINDING_CancellationReturnsNilError(t *testing.T) {
	var rows []string
	for i := 0; i < 50; i++ {
		rows = append(rows, fmt.Sprintf("e%d,1,USD,r,d,2026-09-28T10:00:00Z", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &collector{}
	n, err := ProcessCSV(ctx, "up_1", "src", strings.NewReader(csvBody(csvHeader, rows...)), c.store)
	require.NoError(t, err, "cancellation is not reported as an error")
	assert.Equal(t, 0, n)
	assert.Empty(t, c.txns)
}

func TestProcessCSV_LargeInputSpansMultipleBatches(t *testing.T) {
	// More rows than DefaultWorkerCount full batches guarantees at least one
	// worker flushes a full batch as well as a trailing partial one.
	count := DefaultWorkerCount*DefaultBatchSize + 37
	var b strings.Builder
	b.WriteString(csvHeader + "\n")
	for i := 0; i < count; i++ {
		fmt.Fprintf(&b, "e%05d,1,USD,r,d,2026-09-28T10:00:00Z\n", i)
	}

	var mu sync.Mutex
	seen := make(map[string]int, count)
	store := func(_ context.Context, _ string, txn model.ExternalTransaction) error {
		mu.Lock()
		defer mu.Unlock()
		seen[txn.ID]++
		return nil
	}
	n, err := ProcessCSV(context.Background(), "up_1", "src", strings.NewReader(b.String()), store)
	require.NoError(t, err)
	assert.Equal(t, count, n)
	assert.Len(t, seen, count, "every row stored")
	for id, times := range seen {
		if times != 1 {
			t.Errorf("row %s stored %d times", id, times)
		}
	}
}

// FINDING: ProcessCSV deadlocks when more rows fail than the error channel can
// buffer (min(runtime.NumCPU(), DefaultWorkerCount) entries). Workers block
// sending to errChan, which is only drained after wg.Wait(), so the call never
// returns and context cancellation does not help. Skipped by default because
// the blocked goroutines are leaked; run with:
//
//	RUN_HANG_TEST=1 go test -run TestProcessCSV_FINDING_ManyBadRowsHangs -v ./internal/files/
func TestProcessCSV_FINDING_ManyBadRowsHangs(t *testing.T) {
	if os.Getenv("RUN_HANG_TEST") != "1" {
		t.Skip("documents a known deadlock; set RUN_HANG_TEST=1 to run")
	}

	// Rows lack Currency/Reference/Description, so every one fails parsing.
	var rows []string
	for i := 0; i < DefaultWorkerCount+6; i++ {
		rows = append(rows, fmt.Sprintf("e%d,1,2026-09-28T10:00:00Z", i))
	}
	body := csvBody("ID,Amount,Date", rows...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = ProcessCSV(ctx, "up_1", "src", strings.NewReader(body), (&collector{}).store)
	}()

	select {
	case <-done:
		t.Fatal("ProcessCSV returned; the deadlock appears to be fixed, update this test")
	case <-time.After(time.Second):
	}

	cancel()
	select {
	case <-done:
		t.Fatal("ProcessCSV returned after cancel; the deadlock appears to be fixed, update this test")
	case <-time.After(time.Second):
	}
}
