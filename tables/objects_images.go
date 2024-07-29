package tables

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tidwall/gjson"
	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-feature/alt"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
)

type ObjectsImagesTable struct {
	wof_sql.Table
	name string
}

func NewObjectsImagesTableWithDatabase(ctx context.Context, db wof_sql.Database) (wof_sql.Table, error) {

	t, err := NewObjectsImagesTable(ctx)

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(ctx, db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func NewObjectsImagesTable(ctx context.Context) (wof_sql.Table, error) {

	t := ObjectsImagesTable{
		name: "objects_images",
	}

	return &t, nil
}

func (t *ObjectsImagesTable) Name() string {
	return t.name
}

func (t *ObjectsImagesTable) Schema() string {

	// sfomuseum-www-collection:schema
	return ""
}

func (t *ObjectsImagesTable) InitializeTable(ctx context.Context, db wof_sql.Database) error {

	// TBD...
	// return utils.CreateTableIfNecessary(db, t)

	return nil
}

func (t *ObjectsImagesTable) IndexRecord(ctx context.Context, db wof_sql.Database, i interface{}, custom ...interface{}) error {

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

func (t *ObjectsImagesTable) IndexFeature(ctx context.Context, tx *sql.Tx, body []byte, custom ...interface{}) error {

	if alt.IsAlt(body) {
		return nil
	}

	id, err := properties.Id(body)

	if err != nil {
		return fmt.Errorf("Failed to determine ID, %w", err)
	}

	fl, err := properties.IsDeprecated(body)

	if err != nil {
		return fmt.Errorf("Failed to determined whether ID %d is deprecated, %w", id, err)
	}

	if fl.StringFlag() == "1" {
		return nil
	}

	object_id, err := properties.ParentId(body)

	if err != nil {
		return fmt.Errorf("Failed to determine ID, %w", err)
	}

	lastmod := properties.LastModified(body)

	if lastmod < 0 {
		lastmod = 0
	}

	status_id := int64(0)
	is_primary := int64(0)
	index := int64(0)

	col_id := int64(0)
	cat_id := int64(0)
	subcat_id := int64(0)

	col_rsp := gjson.GetBytes(body, "properties.millsfield:collection_id")

	if col_rsp.Exists() {
		col_id = col_rsp.Int()
	}

	cat_rsp := gjson.GetBytes(body, "properties.millsfield:category_id")

	if cat_rsp.Exists() {
		cat_id = cat_rsp.Int()
	}

	subcat_rsp := gjson.GetBytes(body, "properties.millsfield:subcategory_id")

	if subcat_rsp.Exists() {
		subcat_id = subcat_rsp.Int()
	}

	status_rsp := gjson.GetBytes(body, "properties.media:status_id")

	if status_rsp.Exists() {
		status_id = status_rsp.Int()
	}

	primary_rsp := gjson.GetBytes(body, "properties.sfomuseum:primary_image")

	if primary_rsp.Exists() {
		is_primary = primary_rsp.Int()
	}

	index_rsp := gjson.GetBytes(body, "properties.sfomuseum:image_index")

	if index_rsp.Exists() {
		index = index_rsp.Int()
	}

	q := fmt.Sprintf(`REPLACE INTO %s (
		id, object_id, collection_id, category_id, subcategory_id, status_id, is_primary, image_index, lastmodified
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?
	)`, t.Name())

	_, err = tx.ExecContext(ctx, q, id, object_id, col_id, cat_id, subcat_id, status_id, is_primary, index, lastmod)

	if err != nil {
		return fmt.Errorf("Failed to execute database command, %w", err)
	}

	return nil
}
