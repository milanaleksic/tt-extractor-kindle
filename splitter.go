package tt_extractor_kindle

import (
	"bufio"
	"bytes"
	"io"
)

const maxBlockSize = 1024 * 1024
const buffSize = 64 * 1024
const kindleSplitter = "=========="

func configureScanner(reader io.Reader) (scanner *bufio.Scanner) {
	scanner = bufio.NewScanner(reader)
	buf := make([]byte, 0, buffSize)
	scanner.Buffer(buf, maxBlockSize)
	scanner.Split(kindleRecordSplitter)
	return
}

func kindleRecordSplitter(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	separator := []byte(kindleSplitter)
	if i := bytes.Index(data, separator); i >= 0 {
		nbs := skipWhitespace(data, i+1+len(separator)) // next block start
		cbs := skipWhitespace(data, 0)                  // current block start
		cbe := beforeWhitespace(data, i)                // current block ending + 1
		return nbs, data[cbs:cbe], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

func beforeWhitespace(data []byte, startFrom int) int {
	iter := startFrom
	for iter > 0 && (data[iter-1] == '\n' || data[iter-1] == '\r') {
		iter--
	}
	return iter
}

func skipWhitespace(data []byte, startFrom int) int {
	iter := startFrom
	for {
		if iter < len(data) && (data[iter] == '\n' || data[iter] == '\r') {
			iter++
		} else if iter < len(data)-2 && bytes.Equal(data[iter:iter+3], []byte{0xEF, 0xBB, 0xBF}) {
			iter += 3
		}
		break
	}
	return iter
}
