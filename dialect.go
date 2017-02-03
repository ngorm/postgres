package postgres

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ngorm/common"
	"github.com/ngorm/ngorm/model"
)

type Postgres struct {
	common.Dialect
}

func (Postgres) GetName() string {
	return "postgres"
}

func (Postgres) BindVar(i int) string {
	return fmt.Sprintf("$%v", i)
}

func (Postgres) DataTypeOf(field *model.StructField) (string, error) {
	dataValue, sqlType, size, additionalType :=
		model.ParseFieldStructForDialect(field)
	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "boolean"
		case reflect.Int, reflect.Int8,
			reflect.Int16, reflect.Int32,
			reflect.Uint, reflect.Uint8,
			reflect.Uint16, reflect.Uint32,
			reflect.Uintptr:
			if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok || field.IsPrimaryKey {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = "serial"
			} else {
				sqlType = "integer"
			}
		case reflect.Int64, reflect.Uint64:
			if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok || field.IsPrimaryKey {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = "bigserial"
			} else {
				sqlType = "bigint"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "numeric"
		case reflect.String:
			if _, ok := field.TagSettings["SIZE"]; !ok {
				size = 0 // if SIZE haven't been set, use `text` as the default type, as there are no performance different
			}

			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("varchar(%d)", size)
			} else {
				sqlType = "text"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "timestamp with time zone"
			}
		case reflect.Map:
			if dataValue.Type().Name() == "Hstore" {
				sqlType = "hstore"
			}
		default:
			if isByteArrayOrSlice(dataValue) {
				sqlType = "bytea"
			} else if isUUID(dataValue) {
				sqlType = "uuid"
			}
		}
	}

	if sqlType == "" {
		return "", fmt.Errorf("invalid sql type %s (%s) for postgres",
			dataValue.Type().Name(), dataValue.Kind().String())
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType, nil
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType), nil
}

func (s Postgres) HasIndex(tableName string, indexName string) bool {
	var count int
	s.DB.QueryRow(
		"SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2",
		tableName, indexName).Scan(&count)
	return count > 0
}

func (s Postgres) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	query := `
SELECT Count(con.conname)
FROM   pg_constraint con
WHERE  $1 :: regclass :: oid = con.conrelid
       AND con.conname = $2
       AND con.contype = 'f'
	`
	s.DB.QueryRow(query, tableName, foreignKeyName).Scan(&count)
	return count > 0
}

func (s Postgres) HasTable(tableName string) bool {
	var count int
	query := `
SELECT Count(*)
FROM   information_schema.tables
WHERE  table_name = $1
       AND table_type = 'BASE TABLE'
	`
	s.DB.QueryRow(query, tableName).Scan(&count)
	return count > 0
}

func (s Postgres) HasColumn(tableName string, columnName string) bool {
	var count int
	query := `
SELECT Count(*)
FROM   information_schema.columns
WHERE  table_name = $1
       AND column_name = $2
	`
	s.DB.QueryRow(query, tableName, columnName).Scan(&count)
	return count > 0
}

func (s Postgres) CurrentDatabase() (name string) {
	s.DB.QueryRow("SELECT CURRENT_DATABASE()").Scan(&name)
	return
}

func (s Postgres) LastInsertIDReturningSuffix(tableName, key string) string {
	return fmt.Sprintf("RETURNING %v.%v", tableName, key)
}

func (Postgres) SupportLastInsertID() bool {
	return false
}

func isByteArrayOrSlice(value reflect.Value) bool {
	return (value.Kind() == reflect.Array || value.Kind() == reflect.Slice) && value.Type().Elem() == reflect.TypeOf(uint8(0))
}

func isUUID(value reflect.Value) bool {
	if value.Kind() != reflect.Array || value.Type().Len() != 16 {
		return false
	}
	typename := value.Type().Name()
	lower := strings.ToLower(typename)
	return "uuid" == lower || "guid" == lower
}
