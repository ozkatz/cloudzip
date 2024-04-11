package zipfile

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"time"
)

const (
	EOCDPrefetchBufferSize = 65536 // 64kb is more than enough
	Zip64HeaderId          = 0x0001
)

var (
	EOCDSignature   = []byte{0x50, 0x4b, 0x05, 0x06}
	EOCD64Signature = []byte{0x50, 0x4b, 0x06, 0x06}
)

var (
	ErrInvalidZip   = errors.New("invalid zip file")
	ErrFileNotFound = errors.New("file not found")
)

type EOCD struct {
	Signature         uint32
	CurrentDiskNumber uint16
	CDDiskNumber      uint16
	DiskCDRs          uint16
	TotalCDRs         uint16
	CDSizeBytes       uint32
	CDByteOffset      uint32
}

type EOCD64 struct {
	Signature              uint32
	SizeBytes              uint64
	CreatorVersion         uint16
	VersionNeededToExtract uint16
	CurrentDiskNumber      uint32
	CDDiskNumber           uint32
	DiskCDRs               uint64
	TotalCDRs              uint64
	CDSizeBytes            uint64
	CDByteOffset           uint64
}

type cdrMetadata struct {
	FileHeaderSignature      uint32
	CreatorVersion           uint16
	VersionNeededToExtract   uint16
	GeneralPurposeBitFlag    uint16
	CompressionMethod        uint16
	ModTime                  uint16
	ModDate                  uint16
	CRC32Uncompressed        uint32
	CompressedSizeBytesRaw   uint32
	UncompressedSizeBytesRaw uint32
	FileNameLength           uint16
	ExtraFieldLength         uint16
	FileCommentLength        uint16
	FileStartDiskNumberRaw   uint16
	InternalFileAttributes   uint16
	ExternalFileAttributes   uint32
	LocalFileHeaderOffsetRaw uint32
}

type zip64ExtraFields struct {
	UncompressedSizeBytes uint64
	CompressedSizeBytes   uint64
	LocalFileHeaderOffset uint64
	FileStartDiskNumber   uint32
}

type localHeader struct {
	Signature                uint32
	VersionNeededToExtract   uint16
	GeneralPurposeBitFlag    uint16
	CompressionMethod        uint16
	ModTime                  uint16
	ModDate                  uint16
	CRC32Uncompressed        uint32
	CompressedSizeBytesRaw   uint32
	UncompressedSizeBytesRaw uint32
	FileNameLength           uint16
	ExtraFieldLength         uint16
}

type CDR struct {
	CompressionMethod     uint16
	Modified              time.Time
	CRC32Uncompressed     uint32
	CompressedSizeBytes   uint64
	UncompressedSizeBytes uint64
	Mode                  fs.FileMode
	LocalFileHeaderOffset uint64
	FileName              string
	ExtraFields           []byte
	FileComment           []byte
}

type CDLocation struct {
	SizeBytes uint64
	Offset    uint64
	Zip64     bool
}

type OffsetFetcher interface {
	Fetch(start, end *int64) (io.Reader, error)
}

type CentralDirectoryParser struct {
	reader OffsetFetcher
}

func NewCentralDirectoryParser(reader OffsetFetcher) *CentralDirectoryParser {
	return &CentralDirectoryParser{
		reader: reader,
	}
}

func (p *CentralDirectoryParser) getEOCDBuffer() ([]byte, error) {
	var bufSize int64 = EOCDPrefetchBufferSize
	r, err := p.reader.Fetch(nil, &bufSize)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

func (p *CentralDirectoryParser) getCDLocation() (*CDLocation, error) {
	buf, err := p.getEOCDBuffer()
	if err != nil {
		return nil, err
	}
	eocdStartOffset := bytes.LastIndex(buf, EOCDSignature)
	if eocdStartOffset == -1 {
		// no signature found!
		return nil, ErrInvalidZip
	}
	eocd := &EOCD{}
	err = binary.Read(bytes.NewReader(buf[eocdStartOffset:]), binary.LittleEndian, eocd)
	if err != nil {
		return nil, ErrInvalidZip
	}
	// check if zip64
	if eocd.CurrentDiskNumber == 0xffff ||
		eocd.CDDiskNumber == 0xffff ||
		eocd.DiskCDRs == 0xffff ||
		eocd.TotalCDRs == 0xffff ||
		eocd.CDByteOffset == 0xffffffff ||
		eocd.CDSizeBytes == 0xffffffff {
		return p.getCD64Location(buf)
	}

	return &CDLocation{
		SizeBytes: uint64(eocd.CDSizeBytes),
		Offset:    uint64(eocd.CDByteOffset),
		Zip64:     false,
	}, nil
}

func (p *CentralDirectoryParser) getCD64Location(buf []byte) (*CDLocation, error) {
	eocdStartOffset := bytes.LastIndex(buf, EOCD64Signature)
	if eocdStartOffset == -1 {
		// no signature found!
		return nil, ErrInvalidZip
	}
	eocd := &EOCD64{}
	err := binary.Read(bytes.NewReader(buf[eocdStartOffset:]), binary.LittleEndian, eocd)
	if err != nil {
		return nil, ErrInvalidZip
	}

	return &CDLocation{
		SizeBytes: eocd.CDSizeBytes,
		Offset:    eocd.CDByteOffset,
		Zip64:     true,
	}, nil
}

func parseZip64ExtraFields(extraFields []byte) *zip64ExtraFields {
	var ef zip64ExtraFields
	zip64Offset := -1
	var i int
	for i < len(extraFields) {
		header := binary.LittleEndian.Uint16(extraFields[i : i+2])
		if header == Zip64HeaderId {
			zip64Offset = i
			break
		}
		// otherwise, read next 2 bytes and skip to next header
		incrementOffset := binary.LittleEndian.Uint16(extraFields[i+2 : i+4])
		i += 4 + int(incrementOffset)
	}
	if zip64Offset == -1 {
		return nil
	}
	zip64ChunkSize := binary.LittleEndian.Uint16(extraFields[zip64Offset+2 : zip64Offset+4])
	if zip64ChunkSize >= 8 {
		ef.UncompressedSizeBytes = binary.LittleEndian.Uint64(extraFields[zip64Offset+4 : zip64Offset+12])
	}
	if zip64ChunkSize >= 16 {
		ef.CompressedSizeBytes = binary.LittleEndian.Uint64(extraFields[zip64Offset+12 : zip64Offset+20])
	}
	if zip64ChunkSize >= 24 {
		ef.LocalFileHeaderOffset = binary.LittleEndian.Uint64(extraFields[zip64Offset+20 : zip64Offset+28])
	}
	if zip64ChunkSize >= 32 {
		ef.FileStartDiskNumber = binary.LittleEndian.Uint32(extraFields[zip64Offset+28 : zip64Offset+32])
	}
	return &ef
}

func offset(n uint64) *int64 {
	a := int64(n)
	return &a
}

func ReadCDR(r io.Reader) (*CDR, error) {
	cdr := &CDR{}
	metadata := &cdrMetadata{}
	err := binary.Read(r, binary.LittleEndian, metadata)
	if err != nil {
		return nil, err
	}
	cdr.CRC32Uncompressed = metadata.CRC32Uncompressed
	cdr.CompressionMethod = metadata.CompressionMethod
	cdr.Modified = msDosTimeToTime(metadata.ModDate, metadata.ModTime)

	var mode fs.FileMode
	switch metadata.CreatorVersion >> 8 {
	case creatorUnix, creatorMacOSX:
		mode = unixModeToFileMode(metadata.ExternalFileAttributes >> 16)
	case creatorNTFS, creatorVFAT, creatorFAT:
		mode = msdosModeToFileMode(metadata.ExternalFileAttributes)
	}

	fileNameBuffer := make([]byte, metadata.FileNameLength)
	if metadata.FileNameLength > 0 {
		_, err = r.Read(fileNameBuffer)
		if err != nil {
			return nil, err
		}
	}

	extraFieldBuffer := make([]byte, metadata.ExtraFieldLength)
	if metadata.ExtraFieldLength > 0 {
		_, err = r.Read(extraFieldBuffer)
		if err != nil {
			return nil, err
		}
	}

	fileCommentBuffer := make([]byte, metadata.FileCommentLength)
	if metadata.FileCommentLength > 0 {
		_, err = r.Read(fileCommentBuffer)
		if err != nil {
			return nil, err
		}
	}

	cdr.FileName = string(fileNameBuffer)
	cdr.ExtraFields = extraFieldBuffer
	cdr.FileComment = fileCommentBuffer

	zip64Fields := parseZip64ExtraFields(cdr.ExtraFields)

	if metadata.UncompressedSizeBytesRaw == 0xffffffff {
		// zip64
		if zip64Fields == nil {
			return nil, ErrInvalidZip
		}
		cdr.UncompressedSizeBytes = zip64Fields.UncompressedSizeBytes
	} else {
		cdr.UncompressedSizeBytes = uint64(metadata.UncompressedSizeBytesRaw)
	}

	if metadata.CompressedSizeBytesRaw == 0xffffffff {
		// zip64
		if zip64Fields == nil {
			return nil, ErrInvalidZip
		}
		cdr.CompressedSizeBytes = zip64Fields.CompressedSizeBytes
	} else {
		cdr.CompressedSizeBytes = uint64(metadata.CompressedSizeBytesRaw)
	}

	if metadata.LocalFileHeaderOffsetRaw == 0xffffffff {
		//  zip64
		if zip64Fields == nil {
			return nil, ErrInvalidZip
		}
		cdr.LocalFileHeaderOffset = zip64Fields.LocalFileHeaderOffset
	} else {
		cdr.LocalFileHeaderOffset = uint64(metadata.LocalFileHeaderOffsetRaw)
	}

	if len(cdr.FileName) > 0 && cdr.FileName[len(cdr.FileName)-1] == '/' {
		mode |= fs.ModeDir
	}
	cdr.Mode = mode
	return cdr, nil
}

func (p *CentralDirectoryParser) parseCDR(loc *CDLocation) ([]*CDR, error) {
	reader, err := p.reader.Fetch(offset(loc.Offset), offset(loc.Offset+loc.SizeBytes))
	if err != nil {
		return nil, ErrInvalidZip
	}
	start := time.Now()
	buf, err := io.ReadAll(reader)
	slog.Debug("read Central Directory",
		"size_bytes", len(buf), "took_ms", time.Since(start).Milliseconds())
	if err != nil {
		return nil, err
	}

	parsingStart := time.Now()
	r := bytes.NewReader(buf)
	records := make([]*CDR, 0)
	var pos int64
	for pos < int64(loc.SizeBytes) {
		cdr, err := ReadCDR(r)
		if err != nil {
			return nil, err
		}
		records = append(records, cdr)
		pos, err = r.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
	}
	slog.Debug("parse Central Directory",
		"records", len(records), "took_ms", time.Since(parsingStart).Milliseconds())
	return records, nil
}

func (p *CentralDirectoryParser) GetCentralDirectory() ([]*CDR, error) {
	loc, err := p.getCDLocation()
	if err != nil {
		return nil, err
	}
	return p.parseCDR(loc)
}

func (p *CentralDirectoryParser) readerForRecord(f *CDR) (io.Reader, error) {
	// found record!
	off := f.LocalFileHeaderOffset
	approxHeaderSize := uint64(p.localHeaderSizeHeuristic(f.FileName))
	approxTotalSize := f.CompressedSizeBytes + approxHeaderSize

	// open a reader at offset
	dataReader, err := p.reader.Fetch(offset(off), offset(off+approxTotalSize))
	if err != nil {
		return nil, err
	}
	h := &localHeader{}
	err = binary.Read(dataReader, binary.LittleEndian, h)
	if err != nil {
		return nil, ErrInvalidZip
	}

	// read local header
	bodyStartsAt := h.ExtraFieldLength + h.FileNameLength
	n, err := dataReader.Read(make([]byte, bodyStartsAt))
	if err != nil || n != int(bodyStartsAt) {
		return nil, ErrInvalidZip
	}
	// limit reader to the size of the compressed bytes
	dataReader = io.LimitReader(dataReader, int64(f.CompressedSizeBytes))

	// now we should have a stream of the body, let's see if we have need to inflate it:
	if f.CompressionMethod == zip.Deflate {
		return flate.NewReader(dataReader), nil
	}
	return dataReader, nil
}

func (p *CentralDirectoryParser) Read(fileName string) (io.Reader, error) {
	directory, err := p.GetCentralDirectory()
	if err != nil {
		return nil, err
	}
	for _, f := range directory {
		if f.FileName == fileName {
			return p.readerForRecord(f)
		}
	}
	return nil, ErrFileNotFound
}

func (p *CentralDirectoryParser) localHeaderSizeHeuristic(filename string) int64 {
	nameLength := len([]byte(filename))
	headerSize := int64(30 + nameLength) // we are at the extra field, not knowing its size
	return headerSize + 1024             // assume 1k variable length field as worst case
}
