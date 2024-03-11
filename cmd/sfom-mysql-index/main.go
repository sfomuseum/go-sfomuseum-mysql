package main

import (
	_ "github.com/sfomuseum/go-sfomuseum-mysql/writer"
)

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git/v2"
)

import (
	"context"
	"log"

	"github.com/whosonfirst/go-whosonfirst-iterwriter/application/iterwriter"	
)

func main() {

	ctx := context.Background()
	logger := log.Default()

	err := iterwriter.Run(ctx, logger)

	if err != nil {
		logger.Fatalf("Failed to iterate, %v", err)
	}

}
