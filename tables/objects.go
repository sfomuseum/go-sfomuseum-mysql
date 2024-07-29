package tables

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tidwall/gjson"
	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
)

type ObjectsTable struct {
	wof_sql.Table
	name string
}

func NewObjectsTableWithDatabase(ctx context.Context, db wof_sql.Database) (wof_sql.Table, error) {

	t, err := NewObjectsTable(ctx)

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(ctx, db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func NewObjectsTable(ctx context.Context) (wof_sql.Table, error) {

	t := ObjectsTable{
		name: "objects",
	}

	return &t, nil
}

func (t *ObjectsTable) Name() string {
	return t.name
}

func (t *ObjectsTable) Schema() string {
	// sfomuseum-www-collection:schema
	return ""
}

func (t *ObjectsTable) InitializeTable(ctx context.Context, db wof_sql.Database) error {

	// TBD...
	// return utils.CreateTableIfNecessary(db, t)

	return nil
}

func (t *ObjectsTable) IndexRecord(ctx context.Context, db wof_sql.Database, i interface{}, custom ...interface{}) error {
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

func (t *ObjectsTable) IndexFeature(ctx context.Context, tx *sql.Tx, body []byte, custom ...interface{}) error {

	id, err := properties.Id(body)

	if err != nil {
		return fmt.Errorf("Failed to determine ID, %w", err)
	}

	title, err := properties.Name(body)

	if err != nil {
		// alt files will not have name
		// return fmt.Errorf("Failed to determine name, %w", err)
	}

	created := properties.Created(body)

	if created < 0 {
		created = 0
	}

	lastmod := properties.LastModified(body)

	if lastmod < 0 {
		lastmod = 0
	}

	col_id := int64(0)
	cat_id := int64(0)
	subcat_id := int64(0)
	date_lower := "0000-01-01"
	date_upper := "0000-01-01"
	date_acquired := "0000-01-01"
	count_images := 0

	airline_name := ""
	airport_name := ""
	aircraft_name := ""

	collection_name := ""
	category_name := ""
	subcategory_name := ""

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

	collection_rsp := gjson.GetBytes(body, "properties.sfomuseum:collection")

	if collection_rsp.Exists() {
		collection_name = collection_rsp.String()
	}

	category_rsp := gjson.GetBytes(body, "properties.sfomuseum:category")

	if category_rsp.Exists() {
		category_name = category_rsp.String()
	}

	subcategory_rsp := gjson.GetBytes(body, "properties.sfomuseum:subcategory")

	if subcategory_rsp.Exists() {
		subcategory_name = subcategory_rsp.String()
	}

	//

	lower_rsp := gjson.GetBytes(body, "properties.date:inception_lower")

	if lower_rsp.Exists() {
		date_lower = lower_rsp.String()
	}

	upper_rsp := gjson.GetBytes(body, "properties.date:cessation_upper")

	if upper_rsp.Exists() {
		date_upper = upper_rsp.String()
	}

	acquired_rsp := gjson.GetBytes(body, "properties.sfomuseum:date_acquired")

	if upper_rsp.Exists() {

		if acquired_rsp.String() != "" {
			date_acquired = acquired_rsp.String()
		}
	}

	images_rsp := gjson.GetBytes(body, "properties.millsfield:images")

	if images_rsp.Exists() {
		count_images = len(images_rsp.Array())
	}

	// inception, cessation

	// airline, airport, aircraft

	airline_rsp := gjson.GetBytes(body, "properties.sfomuseum:airline")

	if airline_rsp.Exists() {
		airline_name = airline_rsp.String()
	}

	airport_rsp := gjson.GetBytes(body, "properties.sfomuseum:airport")

	if airport_rsp.Exists() {
		airport_name = airport_rsp.String()
	}

	aircraft_rsp := gjson.GetBytes(body, "properties.sfomuseum:aircraft")

	if aircraft_rsp.Exists() {
		aircraft_name = aircraft_rsp.String()
	}

	q := fmt.Sprintf(`REPLACE INTO %s (
		id, title, collection_id, collection_name, category_id, category_name, subcategory_id, subcategory_name, airline_name, aircraft_name, airport_name, count_images, date_lower, date_upper, date_acquired, created, lastmodified
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
	)`, t.Name())

	_, err = tx.ExecContext(ctx, q, id, title, col_id, collection_name, cat_id, category_name, subcat_id, subcategory_name, airline_name, aircraft_name, airport_name, count_images, date_lower, date_upper, date_acquired, created, lastmod)

	if err != nil {
		return fmt.Errorf("Failed to execute SQL statement, %w", err)
	}

	return nil
}
