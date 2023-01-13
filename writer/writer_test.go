package writer

import (
	"context"
	wof_writer "github.com/whosonfirst/go-writer/v3"
	"testing"
)

func TestWriter(t *testing.T) {

	ctx := context.Background()

	wr_uri := "sfom.mysql://images?dsn=user:pass@tcp(example.com)/sfomuseum_collection"

	_, err := wof_writer.NewWriter(ctx, wr_uri)

	if err != nil {
		t.Fatalf("Failed to create new writer for '%s', %v", wr_uri, err)
	}

}
