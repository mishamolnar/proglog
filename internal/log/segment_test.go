package log

import (
	log_v1 "github.com/mishamolnar/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	c := Config{}
	c.Segment.MaxIndexBytes = entWidth * 3
	c.Segment.MaxStoreBytes = 1024
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
	require.Equal(t, uint64(16), s.nextOffset)

	want := log_v1.Record{
		Value: []byte("hello world"),
	}

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(&want)
		require.NoError(t, err)
		require.Equal(t, s.baseOffset+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, got.Value, want.Value)
		require.Equal(t, got.Offset, want.Offset)
	}

}
