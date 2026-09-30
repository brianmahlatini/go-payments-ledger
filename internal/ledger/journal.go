package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// FileJournal is an append-only JSON-lines log. Every Append is fsynced
// before returning, so an acknowledged transfer survives power loss.
//
// Trade-off: one fsync per transfer caps throughput at the disk's sync rate
// (hundreds to low thousands/sec on SSDs). Group commit (batching several
// events per fsync) is the standard next step if that ceiling is reached.
type FileJournal struct {
	f *os.File
}

// OpenFileJournal opens (or creates) the journal and returns the events it
// already contains. A torn final line - a crash mid-write - is truncated,
// because it was never acknowledged. Corruption anywhere else is an error:
// silently dropping acknowledged money movements is never acceptable.
func OpenFileJournal(path string) (*FileJournal, []Event, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open journal: %w", err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("read journal: %w", err)
	}

	var events []Event
	good := 0
	for good < len(data) {
		nl := bytes.IndexByte(data[good:], '\n')
		if nl < 0 {
			break // torn tail, handled below
		}
		line := data[good : good+nl]
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("%w: undecodable line at byte %d: %v", ErrJournalCorrupt, good, err)
		}
		events = append(events, ev)
		good += nl + 1
	}
	if good < len(data) {
		if err := f.Truncate(int64(good)); err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("truncate torn tail: %w", err)
		}
	}
	if _, err := f.Seek(int64(good), io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &FileJournal{f: f}, events, nil
}

func (j *FileJournal) Append(ev Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := j.f.Write(b); err != nil {
		return err
	}
	return j.f.Sync()
}

func (j *FileJournal) Close() error {
	if j == nil || j.f == nil {
		return errors.New("journal not open")
	}
	return j.f.Close()
}
