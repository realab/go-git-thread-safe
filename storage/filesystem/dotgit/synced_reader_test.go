package dotgit

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/memfs"
)

func newMemorySyncedReader(t *testing.T) *syncedReader {
	t.Helper()
	fs := memfs.New()
	w, err := fs.Create("pack")
	if err != nil {
		t.Fatal(err)
	}
	r, err := fs.Open("pack")
	if err != nil {
		w.Close()
		t.Fatal(err)
	}
	s := newSyncedReader(w, r)
	t.Cleanup(func() {
		s.Close()
		r.Close()
		w.Close()
	})
	return s
}

func TestSyncedReaderConcurrentMemoryIO(t *testing.T) {
	s := newMemorySyncedReader(t)
	want := bytes.Repeat([]byte("0123456789"), 4096)
	done := make(chan error, 1)
	go func() {
		defer s.Close()
		for start := 0; start < len(want); {
			end := start + 37
			if end > len(want) {
				end = len(want)
			}
			n, err := s.Write(want[start:end])
			if err != nil {
				done <- err
				return
			}
			if n != end-start {
				done <- io.ErrShortWrite
				return
			}
			start = end
		}
		done <- nil
	}()
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read %d bytes, want %d identical bytes", len(got), len(want))
	}
}

func TestSyncedReaderSeekCurrent(t *testing.T) {
	s := newMemorySyncedReader(t)
	if _, err := s.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	if _, err := io.ReadFull(s, buf); err != nil {
		t.Fatal(err)
	}
	if pos, err := s.Seek(-3, io.SeekCurrent); err != nil || pos != 3 {
		t.Fatalf("seek = %d, %v; want 3", pos, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(s, buf[:3])
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil || string(buf[:3]) != "def" {
			t.Fatalf("read = %q, %v; want def", buf[:3], err)
		}
	case <-time.After(5 * time.Second):
		s.Close()
		<-done
		t.Fatal("read blocked despite unread data after relative seek")
	}
}

func TestSyncedReaderCloseUnblocksRead(t *testing.T) {
	s := newMemorySyncedReader(t)
	done := make(chan error, 1)
	go func() {
		_, err := s.Read(make([]byte, 1))
		done <- err
	}()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("read after close = %v, want EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read remained blocked after close")
	}
	if _, err := s.Write([]byte("late")); err != io.ErrClosedPipe {
		t.Fatalf("write after close = %v, want ErrClosedPipe", err)
	}
}
