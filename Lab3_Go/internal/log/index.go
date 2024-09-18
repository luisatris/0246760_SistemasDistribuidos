package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(file *os.File, config Config) (*index, error) {
	idx := &index{
		file: file,
	}
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fileInfo.Size())

	if err = os.Truncate(
		file.Name(), int64(config.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := idx.file.Sync(); err != nil {
		return err
	}
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}
	return idx.file.Close()
}

func (idx *index) Read(input int64) (offset uint32, position uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}
	if input == -1 {
		offset = uint32((idx.size / entryWidth) - 1)
	} else {
		offset = uint32(input)
	}
	position = uint64(offset) * entryWidth
	if idx.size < position+entryWidth {
		return 0, 0, io.EOF
	}
	offset = enc.Uint32(idx.mmap[position : position+offsetWidth])
	position = enc.Uint64(idx.mmap[position+offsetWidth : position+entryWidth])
	return offset, position, nil
}

func (idx *index) Write(offset uint32, position uint64) error {
	if uint64(len(idx.mmap)) < idx.size+entryWidth {
		return io.EOF
	}
	enc.PutUint32(idx.mmap[idx.size:idx.size+offsetWidth], offset)
	enc.PutUint64(idx.mmap[idx.size+offsetWidth:idx.size+entryWidth], position)
	idx.size += uint64(entryWidth)
	return nil
}

func (idx *index) Name() string {
	return idx.file.Name()
}
