package log

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	write = "hello world"
)

var width = uint64(len(write)) + lenWidth

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	fmt.Println("Testing Append...")
	testAppend(t, s)
	fmt.Println("Testing Read...")
	testRead(t, s)
	fmt.Println("Testing ReadAt...")
	testReadAt(t, s)

	// Reopen store to test persistence
	fmt.Println("Reopening Store...")
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append([]byte(write))
		require.NoError(t, err)
		fmt.Printf("Appended %d bytes at position %d, expected width: %d\n", n, pos, width*i)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		fmt.Printf("Read data at position %d: %s\n", pos, read)
		require.Equal(t, []byte(write), read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		fmt.Printf("ReadAt header %d bytes at offset %d\n", n, off)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		fmt.Printf("ReadAt data %d bytes at offset %d: %s\n", n, off, b)
		require.Equal(t, []byte(write), b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append([]byte(write))
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)
	fmt.Printf("File size before closing: %d\n", beforeSize)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	fmt.Printf("File size after closing: %d\n", afterSize)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (*os.File, int64, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
