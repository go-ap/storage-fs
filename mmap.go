//go:build mmap

package fs

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const (
	RecordSize  = 100
	RecordCount = 1000000
)

func (r *repo) loadBinFromFile(path string, bmp any) (err error) {
	f, err := r.root.OpenFile(path, os.O_RDONLY, defaultFilePerm)
	if err != nil {
		return err
	}
	mr, err := WrapInMMapReader(f)
	if err != nil {
		r.logger.Warnf("%s not found", path)
		return errors.NewNotFound(asPathErr(err, r.path), "not found")
	}
	defer func() {
		err = mr.Close()
	}()
	if err = gob.NewDecoder(mr).Decode(bmp); err != nil {
		return err
	}
	return nil
}

func (r *repo) writeBinFile(path string, bmp any) error {
	f, err := r.root.OpenFile(path, defaultNewFileFlags, defaultFilePerm)
	if err != nil {
		r.logger.Warnf("%s not found", path)
		return errors.NewNotFound(asPathErr(err, r.path), "not found")
	}
	mf, err := WrapInMMapWriter(f)
	if err != nil {
		r.logger.Warnf("%s not found", path)
		return errors.NewNotFound(asPathErr(err, r.path), "not found")
	}
	defer func() {
		if err := mf.Close(); err != nil {
			r.logger.Warnf("Unable to close file: %s", asPathErr(err, r.path))
		}
	}()
	return gob.NewEncoder(mf).Encode(bmp)
}

func (r *repo) loadBinFromFile(path string, bmp any) (err error) {
	f, err := r.root.OpenFile(path, os.O_RDONLY, defaultFilePerm)
	if err != nil {
		return err
	}
	mr, err := WrapInMMapReader(f)
	if err != nil {
		r.logger.Warnf("%s not found", path)
		return errors.NewNotFound(asPathErr(err, r.path), "not found")
	}
	defer func() {
		err = mr.Close()
	}()
	if err = gob.NewDecoder(mr).Decode(bmp); err != nil {
		return err
	}
	return nil
}

type mmapReader struct {
	file *os.File
	data []byte
}

func WrapInMMapReader(file *os.File) (*mmapReader, error) {
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	data, err := unix.Mmap(int(file.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &mmapReader{
		file: file,
		data: data,
	}, nil
}

func (m *mmapReader) Read(data []byte) (int, error) {
	read, err := m.ReadRecord(0, data)
	if err != nil {
		return 0, err
	}
	cnt := copy(data, read)
	return cnt, nil
}

func (m *mmapReader) ReadRecord(index int, buf []byte) ([]byte, error) {
	if index < 0 || index >= RecordCount {
		return nil, fmt.Errorf("index %d out of range [0, %d)", index, RecordCount)
	}

	offset := index * RecordSize
	if offset+RecordSize > len(m.data) {
		return nil, fmt.Errorf("record %d would exceed file bounds", index)
	}

	return m.data[offset : offset+RecordSize], nil
}

func (m *mmapReader) Close() error {
	var err1, err2 error
	if m.data != nil {
		err1 = unix.Munmap(m.data)
	}
	if m.file != nil {
		err2 = m.file.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

type mmapWriter struct {
	file *os.File
	data []byte
}

func WrapInMMapWriter(file *os.File) (*mmapWriter, error) {
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	data, err := unix.Mmap(int(file.Fd()), 0, int(stat.Size())+1, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file for writing: %w", err)
	}

	return &mmapWriter{
		file: file,
		data: data,
	}, nil
}

func (w *mmapWriter) Write(data []byte) (int, error) {
	if err := w.WriteRecord(0, data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (w *mmapWriter) WriteRecord(index int, data []byte) error {
	if index < 0 || index >= RecordCount {
		return fmt.Errorf("index %d out of range [0, %d)", index, RecordCount)
	}

	if len(data) != RecordSize {
		return fmt.Errorf("data size mismatch: expected %d bytes, got %d", RecordSize, len(data))
	}

	offset := index * RecordSize
	if offset+RecordSize > len(w.data) {
		return fmt.Errorf("record %d would exceed file bounds", index)
	}

	copy(w.data[offset:offset+RecordSize], data)
	return nil
}

func (w *mmapWriter) Close() error {
	var err1, err2 error
	if w.data != nil {
		err1 = unix.Munmap(w.data)
	}
	if w.file != nil {
		err2 = w.file.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func (w *mmapWriter) EvictPages() error {
	if w.data == nil {
		return nil
	}
	return unix.Madvise(w.data, unix.MADV_DONTNEED)
}

func (w *mmapWriter) WarmPages() {
	if w.data == nil {
		return
	}
	pageSize := 4096
	for i := 0; i < len(w.data); i += pageSize {
		_ = w.data[i]
	}
}
