package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "ProyectoGo/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(directory string, config Config) (*Log, error) {
	if config.Segment.MaxStoreBytes == 0 {
		config.Segment.MaxStoreBytes = 1024
	}
	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxIndexBytes = 1024
	}
	log := &Log{
		Dir:    directory,
		Config: config,
	}

	return log, log.setup()
}

func (log *Log) setup() error {
	files, err := ioutil.ReadDir(log.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offsetStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		offset, _ := strconv.ParseUint(offsetStr, 10, 0)
		baseOffsets = append(baseOffsets, offset)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}
	if log.segments == nil {
		if err = log.newSegment(
			log.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	offset, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if log.activeSegment.IsMaxed() {
		err = log.newSegment(offset + 1)
	}
	return offset, err
}

func (log *Log) Read(offset uint64) (*api.Record, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	var selectedSegment *segment
	for _, segment := range log.segments {
		if segment.baseOffset <= offset && offset < segment.nextOffset {
			selectedSegment = segment
			break
		}
	}
	if selectedSegment == nil || selectedSegment.nextOffset <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}
	return selectedSegment.Read(offset)
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}
	return os.RemoveAll(log.Dir)
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

func (log *Log) LowestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	if len(log.segments) == 0 {
		return 0, fmt.Errorf("no segments available")
	}
	return log.segments[0].baseOffset, nil
}

func (log *Log) HighestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	if len(log.segments) == 0 {
		return 0, fmt.Errorf("no segments available")
	}
	offset := log.segments[len(log.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}
	return offset - 1, nil
}

func (log *Log) Truncate(lowest uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	var remainingSegments []*segment
	for _, segment := range log.segments {
		if segment.nextOffset <= lowest+1 {
			if err := segment.Remove(); err != nil {
				return err
			}
			continue
		}
		remainingSegments = append(remainingSegments, segment)
	}
	log.segments = remainingSegments
	return nil
}

func (log *Log) Reader() io.Reader {
	log.mu.RLock()
	defer log.mu.RUnlock()
	readers := make([]io.Reader, len(log.segments))
	for i, segment := range log.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	offset int64
}

func (reader *originReader) Read(p []byte) (int, error) {
	n, err := reader.ReadAt(p, reader.offset)
	reader.offset += int64(n)
	return n, err
}

func (log *Log) newSegment(offset uint64) error {
	segment, err := newSegment(log.Dir, offset, log.Config)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, segment)
	log.activeSegment = segment
	return nil
}
