package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("", "index_test")
	require.NoError(t, err)
	defer os.ReadFile(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
		{Off: 2, Pos: 20},
	}
	for _, val := range entries {
		err = idx.Write(val.Off, val.Pos)
		require.NoError(t, err)
	}
	for i, val := range entries {
		out, pos, err := idx.Read(int64(i))
		require.NoError(t, err)
		require.Equal(t, pos, val.Pos)
		require.Equal(t, out, val.Off)
	}
	// index and scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	// index should build its state from the existing file
	_ = idx.Close()
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	out, pos, err := idx.Read(-1) //reading last entry
	require.Equal(t, entries[len(entries)-1].Pos, pos)
	require.Equal(t, uint32(len(entries)-1), out)
}
