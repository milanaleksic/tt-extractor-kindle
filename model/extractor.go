package model

import (
	"context"
	"io"
)

type Extractor interface {
	IngestRecords(ctx context.Context, reader io.Reader) (err error)
}
