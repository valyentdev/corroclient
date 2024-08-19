package corroclient

import (
	"errors"
	"fmt"
	"sync"
)

var ErrScan = errors.New("corroclient: scan error")

// Warning: Scan does not handle time.Time because of the various ways time can be stored
// in SQLite and JSON. You're responsible for converting time.Time yourself from numbers types or
// strings.
func (r *Row) Scan(dest ...any) error {
	for i, value := range r.values {
		if value == nil {
			continue
		}
		switch v := value.(type) {
		case float64:
			if err := scanJSONNumber(v, dest[i]); err != nil {
				return fmt.Errorf("%w, failed to scan JSON float64 %s", err, value)
			}
			continue
		case string:
			if err := scanJSONString(v, dest[i]); err != nil {
				return fmt.Errorf("%w, failed to scan JSON string %s", err, value)
			}
			continue
		case bool:
			if err := scanJSONBool(v, dest[i]); err != nil {
				return fmt.Errorf("%w, failed to scan JSON bool %s", err, value)
			}
			continue
		}
	}
	return nil
}

type Rows struct {
	columns      []string
	rows         []*Row
	currentIndex int
	mutex        sync.RWMutex
}

func (r *Rows) Columns() []string {
	return r.columns
}

func (r *Rows) Next() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.currentIndex == len(r.rows)-1 {
		return false
	}

	r.currentIndex++
	return true
}

// Warning: Scan does not handle time.Time because of the various ways time can be stored
// in SQLite and JSON. You're responsible for converting time.Time yourself from numbers types or
// strings.
func (r *Rows) Scan(dest ...any) error {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	if r.currentIndex == -1 {
		return fmt.Errorf("you must call Next at least once before calling Scan")
	}

	row := r.rows[r.currentIndex]

	return row.Scan(dest...)
}
