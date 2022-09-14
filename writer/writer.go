package writer

import (
	_ "github.com/go-sql-driver/mysql"
)

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-sfomuseum-mysql/tables"
	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	wof_writer "github.com/whosonfirst/go-writer/v2"
	"io"
	"log"
	"net/url"
)

func init() {
	ctx := context.Background()
	wof_writer.RegisterWriter(ctx, "mysql_sfom", NewMySQLWriter)
}

type MySQLWriter struct {
	wof_writer.Writer
	db     wof_sql.Database
	tables []wof_sql.Table
	logger *log.Logger
}

func NewMySQLWriter(ctx context.Context, uri string) (wof_writer.Writer, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	db_uri := fmt.Sprintf("mysql://?dsn=%s", q.Get("dsn"))

	db, err := wof_sql.NewSQLDB(ctx, db_uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create database, %w", err)
	}

	index_objects := true
	index_images := true

	to_index := make([]wof_sql.Table, 0)

	if index_objects {

		t, err := tables.NewObjectsTableWithDatabase(ctx, db)

		if err != nil {
			return nil, fmt.Errorf("Failed to create objects table, %w", err)
		}

		to_index = append(to_index, t)
	}

	if index_images {

		t, err := tables.NewObjectsImagesTableWithDatabase(ctx, db)

		if err != nil {
			return nil, fmt.Errorf("Failed to create objects images table, %w", err)
		}

		to_index = append(to_index, t)
	}

	logger := log.Default()

	wr := &MySQLWriter{
		db:     db,
		tables: to_index,
		logger: logger,
	}

	return wr, nil
}

func (wr *MySQLWriter) Write(ctx context.Context, path string, r io.ReadSeeker) (int64, error) {

	body, err := io.ReadAll(r)

	if err != nil {
		return 0, fmt.Errorf("Failed to read document, %w", err)
	}

	err = wr.db.IndexFeature(ctx, wr.tables, body)

	if err != nil {
		return 0, fmt.Errorf("Failed to index %s, %w", path, err)
	}

	return 0, nil
}

func (wr *MySQLWriter) WriterURI(ctx context.Context, uri string) string {
	return uri
}

func (wr *MySQLWriter) Flush(ctx context.Context) error {
	return nil
}

func (wr *MySQLWriter) Close(ctx context.Context) error {
	return nil
}

func (wr *MySQLWriter) SetLogger(ctx context.Context, logger *log.Logger) error {
	wr.logger = logger
	return nil
}
