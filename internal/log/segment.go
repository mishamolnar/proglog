package log

import (
	"fmt"
	"github.com/mishamolnar/proglog/api/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strings"
)

// baseOffset is starting global offset. Next relative offset is nextOffset - baseOffset
// config to know when segment is exhausted
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{baseOffset: baseOffset, config: c}
	storeFile, err := os.OpenFile(
		strings.Join([]string{dir, "segment"}, string(filepath.Separator)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		strings.Join([]string{dir, "index"}, string(filepath.Separator)),
		os.O_RDWR|os.O_CREATE,
		0644)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	//initializing offset by retrieving last record from index. If error returned then baseOffset
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *log_v1.Record) (offset uint64, err error) {
	curr := s.nextOffset
	record.Offset = curr
	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(bytes)
	if err != nil {
		return 0, err
	}
	err = s.index.Write(uint32(curr-s.baseOffset), pos) //writing relative offsent
	if err != nil {
		return 0, err
	}
	s.nextOffset++
	return curr, nil
}

func (s *segment) Read(off uint64) (*log_v1.Record, error) {
	if off < s.baseOffset || off > s.nextOffset {
		return nil, fmt.Errorf("offset is out of bounds [%d, %d)", s.baseOffset, s.nextOffset)
	}
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	data, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &log_v1.Record{}
	err = proto.Unmarshal(data, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size > s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
