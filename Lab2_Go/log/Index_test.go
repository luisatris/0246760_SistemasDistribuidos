package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	tempFile, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	config := Config{}
	config.Segment.MaxIndexBytes = 1024
	index, err := newIndex(tempFile, config)
	require.NoError(t, err)

	//Prueba de lectura en el index cuando esta vacio
	fmt.Println("Testing Read on empty index...")
	_, _, err = index.Read(-1)
	require.Error(t, err)
	fmt.Println("Error as expected:", err)

	entries := []struct {
		Offset   uint32
		Position uint64
	}{
		{Offset: 0, Position: 0},
		{Offset: 1, Position: 10},
	}

	fmt.Println("Testing Write and Read...")
	for _, expected := range entries {
		err = index.Write(expected.Offset, expected.Position)
		require.NoError(t, err)
		fmt.Printf("Written Offset: %d, Position: %d\n", expected.Offset, expected.Position)

		_, position, err := index.Read(int64(expected.Offset))
		require.NoError(t, err)
		fmt.Printf("Read Offset: %d, Got Position: %d\n", expected.Offset, position)
		require.Equal(t, expected.Position, position)
	}

	_, _, err = index.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	fmt.Println("EOF as expected:", err)

	_ = index.Close()

	tempFile, err = os.OpenFile(tempFile.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	index, err = newIndex(tempFile, config)
	require.NoError(t, err)

	offset, position, err := index.Read(-1)
	require.NoError(t, err)
	fmt.Printf("Read Offset: %d, Position: %d\n", offset, position)
	require.Equal(t, uint32(1), offset)
	require.Equal(t, entries[1].Position, position)
}
