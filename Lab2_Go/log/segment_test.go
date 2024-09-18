package log

import (
	api "ProyectoGo/api/v1"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	tempDir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(tempDir)

	expectedRecord := &api.Record{Value: []byte("hello world")}

	config := Config{}
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = entryWidth * 3

	segment, err := newSegment(tempDir, 16, config)
	require.NoError(t, err)

	fmt.Printf("Initial nextOffset: %d\n", segment.nextOffset)
	require.Equal(t, uint64(16), segment.nextOffset, "Initial nextOffset mismatch")
	require.False(t, segment.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		offset, err := segment.Append(expectedRecord)
		require.NoError(t, err)
		fmt.Printf("Offset after append %d: %d\n", i, offset)
		require.Equal(t, uint64(16+i), offset, "Offset mismatch after append")

		actualRecord, err := segment.Read(offset)
		require.NoError(t, err)
		require.Equal(t, expectedRecord.Value, actualRecord.Value)
	}

	_, err = segment.Append(expectedRecord)
	require.Equal(t, io.EOF, err)

	require.True(t, segment.IsMaxed())

	config.Segment.MaxStoreBytes = uint64(len(expectedRecord.Value) * 3)
	config.Segment.MaxIndexBytes = 1024

	segment, err = newSegment(tempDir, 16, config)
	require.NoError(t, err)
	require.True(t, segment.IsMaxed())

	err = segment.Remove()
	require.NoError(t, err)
	segment, err = newSegment(tempDir, 16, config)
	require.NoError(t, err)
	require.False(t, segment.IsMaxed())
}
