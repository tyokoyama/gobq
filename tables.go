package gobq

import (
	"fmt"
	"net/http"
	"reflect"

	"code.google.com/p/google-api-go-client/bigquery/v2"
)

type Tables string

func (t Tables)Get(client *http.Client, projectID, datasetID, tableID string) (*bigquery.Table, error) {
	// BigQueryの初期化
	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Println("bigquery.New")
		return nil, err
	}

	tables, err := bq.Tables.Get(projectID, datasetID, tableID).Do()
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func (t Tables)Insert(client *http.Client, projectID, datasetID, tableID string, dest interface{}) (*bigquery.Table, error) {
	// var tableSchema = &bigquery.TableSchema {
	// 		Fields: []*bigquery.TableFieldSchema{},
	// 	}
	var tableSchema bigquery.TableSchema
	var fields []*bigquery.TableFieldSchema

	ty := reflect.TypeOf(dest)
	for i := 0; i < ty.FieldAlign(); i++ {
		var fieldSchema bigquery.TableFieldSchema
		destField := ty.Field(i)

		fmt.Println(destField)
		if destField.Offset > 0 {				// 構造体の中には謎のフィールドがある。
			fieldSchema.Name = destField.Name
			switch destField.Type.Kind() {
			case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				fieldSchema.Type = "integer"
			case reflect.Float32, reflect.Float64:
				fieldSchema.Type = "float"
			case reflect.Bool:
				fieldSchema.Type = "boolean"
			case reflect.Struct:
				if destField.Type.Name() == "time.Time" {		// TODO: time.Timeが来た時の処理とフィールドが構造体の時のテスト
					fieldSchema.Type = "timestamp"
				} else {
					fieldSchema.Type = "record"
				}
			default:
				fieldSchema.Type = "string"
			}

			fields = append(fields, &fieldSchema)
		}
	}
	tableSchema.Fields = fields

	// BigQueryの初期化
	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Println("bigquery.New")
		return nil, err
	}

	var newTable bigquery.Table
	newTable.TableReference = &bigquery.TableReference{DatasetId: datasetID, ProjectId: projectID, TableId: tableID}
	newTable.Schema = &tableSchema

	insRes, err := bq.Tables.Insert(projectID, datasetID, &newTable).Do()
	if err != nil {
		fmt.Println("tables.Insert")
		return nil, err
	}

	return insRes, nil
}