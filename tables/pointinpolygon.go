package tables

// Note the trailing '?maxAllowedPacket=0' on the DSN. That is important any necessary (often
// in conjunction with tweaks to the my.cnf file) to index large records.
// go run cmd/sfom-index-mysql/main.go -writer-uri 'constant://?val=sfom.mysql://pointinpolygon?dsn=foo:bar@/sfomuseum?maxAllowedPacket=0' /usr/local/data/sfomuseum-data-whosonfirst/

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"github.com/paulmach/orb/encoding/wkt"
	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-feature/geometry"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

//go:embed pointinpolygon.schema
var pointinpolygon_schema string

const POINTINPOLYGON_TABLE string = "pointinpolygon"

type PointInPolygonTable struct {
	wof_sql.Table
}

func NewPointInPolygonTableWithDatabase(ctx context.Context, db wof_sql.Database) (wof_sql.Table, error) {

	t, err := NewPointInPolygonTable(ctx)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new pointinpolygon table, %w", err)
	}

	err = t.InitializeTable(ctx, db)

	if err != nil {
		return nil, fmt.Errorf("Failed to initialize pointinpolygon table, %w", err)
	}

	return t, nil
}

func NewPointInPolygonTable(ctx context.Context) (wof_sql.Table, error) {
	t := PointInPolygonTable{}
	return &t, nil
}

func (t *PointInPolygonTable) Name() string {
	return POINTINPOLYGON_TABLE
}

// https://dev.sql.com/doc/refman/8.0/en/json-functions.html
// https://www.percona.com/blog/2016/03/07/json-document-fast-lookup-with-mysql-5-7/
// https://archive.fosdem.org/2016/schedule/event/mysql57_json/attachments/slides/1291/export/events/attachments/mysql57_json/slides/1291/MySQL_57_JSON.pdf

func (t *PointInPolygonTable) Schema() string {
	return pointinpolygon_schema
}

func (t *PointInPolygonTable) InitializeTable(ctx context.Context, db wof_sql.Database) error {
	return wof_sql.CreateTableIfNecessary(ctx, db, t)
}

func (t *PointInPolygonTable) IndexRecord(ctx context.Context, db wof_sql.Database, i interface{}, custom ...interface{}) error {

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to establish database connection, %w", err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})

	if err != nil {
		return fmt.Errorf("Failed to create transaction, %w", err)
	}

	err = t.IndexFeature(ctx, tx, i.([]byte), custom...)

	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Failed to index %s table, %w", t.Name(), err)
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction, %w", err)
	}

	return nil
}

func (t *PointInPolygonTable) IndexFeature(ctx context.Context, tx *sql.Tx, body []byte, custom ...interface{}) error {

	id, err := properties.Id(body)

	if err != nil {
		return fmt.Errorf("Failed to derive ID, %w", err)
	}

	var alt *uri.AltGeom

	if len(custom) >= 1 {
		alt = custom[0].(*uri.AltGeom)
	}

	if alt != nil {
		return nil
	}

	geojson_geom, err := geometry.Geometry(body)

	if err != nil {
		return fmt.Errorf("Failed to derive geometry, %w", err)
	}

	orb_geom := geojson_geom.Geometry()
	wkt_geom := wkt.MarshalString(orb_geom)

	q := fmt.Sprintf(`REPLACE INTO %s (
		geometry, id
	) VALUES (
		ST_GeomFromText('%s'), ?
	)`, POINTINPOLYGON_TABLE, wkt_geom)

	_, err = tx.ExecContext(ctx, q, id)

	if err != nil {
		return fmt.Errorf("Failed to update table, %w", err)
	}

	return nil
}
