package log

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var (
	write = []byte("some log")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.ReadFile(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, width*i, pos+n)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(0); i < 3; i++ {
		data, err := s.Read(i * width)
		require.NoError(t, err)
		require.Equal(t, write, data)
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		bytes := make([]byte, lenWidth)
		n, err := s.ReadAt(bytes, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(bytes)
		bytes = make([]byte, size)
		n, err = s.ReadAt(bytes, off)
		require.NoError(t, err)
		require.Equal(t, bytes, write)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.ReadFile(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	//check stats before closing (and flushing therefore)
	_, sizeBefore, err := openFile(f.Name())
	require.NoError(t, err)

	//check staths after
	err = s.Close()
	_, sizeAfter, err := openFile(f.Name())
	require.NoError(t, err)
	
	require.True(t, sizeAfter > sizeBefore)
}

func openFile(name string) (*os.File, int64, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 06444)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), err
}
