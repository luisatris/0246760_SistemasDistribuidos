package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "ProyectoGo/api/v1"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			config := Config{}
			config.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, config)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := log.Append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	readRecord, err := log.Read(offset)
	require.NoError(t, err)
	require.Equal(t, record.Value, readRecord.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	readRecord, err := log.Read(1)
	require.Nil(t, readRecord)
	require.Error(t, err)
}

func testInitExisting(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(record)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	lowOffset, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowOffset)
	highOffset, err := log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highOffset)

	newLog, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	lowOffset, err = newLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowOffset)
	highOffset, err = newLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highOffset)
}

func testReader(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := log.Append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	bytesRead, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	recordLength := proto.Size(record) + lenWidth
	require.GreaterOrEqual(t, len(bytesRead), recordLength)

	readRecord := &api.Record{}
	err = proto.Unmarshal(bytesRead[lenWidth:], readRecord)
	require.NoError(t, err)
	require.Equal(t, record.Value, readRecord.Value)
}

func testTruncate(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(record)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
