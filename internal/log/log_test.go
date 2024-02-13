package log

import (
	log_v1 "github.com/mishamolnar/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read same record success": testAppendRead,
		"offset out of range error":           testOutOfRangeErr,
		"init with existing segments":         testInitExisting,
		"test log reader":                     testReader,
		"test truncate log":                   testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)
			var c Config
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(tmpDir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	appended := &log_v1.Record{Value: []byte("some log to write")}
	off, err := log.Append(appended)
	require.NoError(t, err)
	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, read.Value, appended.Value)
	require.Equal(t, read.Offset, appended.Offset)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Equal(t, log_v1.ErrOffsetOutOfRange{Offset: 1}, err)
}

func testInitExisting(t *testing.T, log *Log) {
	appended := &log_v1.Record{Value: []byte("Hello world")}
	off, err := log.Append(appended)
	require.NoError(t, err)
	err = log.Close()
	require.NoError(t, err)

	l, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	read, err := l.Read(off)
	require.NoError(t, err)
	require.NotNil(t, read)
	require.Equal(t, read.Value, appended.Value)

	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, read.Offset, off)

	off, err = l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, read.Offset, off)
}

func testReader(t *testing.T, log *Log) {
	appended := log_v1.Record{Value: []byte("Hello world")}
	_, err := log.Append(&appended)
	require.NoError(t, err)
	b, err := io.ReadAll(log.Reader())
	require.NoError(t, err)

	read := &log_v1.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, read.Value, appended.Value)
	require.Equal(t, read.Offset, appended.Offset)
}

func testTruncate(t *testing.T, log *Log) {
	appended := log_v1.Record{Value: []byte("Test value")}
	for i := 0; i < 5; i++ {
		_, err := log.Append(&appended)
		require.NoError(t, err)
	}
	err := log.Truncate(3)
	require.NoError(t, err)
	read, err := log.Read(2)
	require.NoError(t, err)
	require.Equal(t, read.Value, appended.Value)
	loOff, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, loOff, read.Offset)

	hiOff, err := log.HighestOffset()
	require.Equal(t, hiOff, uint64(4))
}
