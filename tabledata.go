package gobq

import (
	"fmt"
	"net/http"
	"reflect"

	"code.google.com/p/google-api-go-client/bigquery/v2"
)

type TableData bigquery.TableDataInsertAllRequest

func(t *TableData) Add(dest interface{}) {
	rowMap := make(map[string]bigquery.JsonValue)

	ty := reflect.TypeOf(dest)
	v := reflect.ValueOf(dest)
	for i := 0; i < ty.FieldAlign(); i++ {
		destField := ty.Field(i)

		if destField.Offset > 0 {				// 構造体の中には謎のフィールドがある。
			switch destField.Type.Kind() {
			case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				rowMap[destField.Name] = v.FieldByName(destField.Name).Int()
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				rowMap[destField.Name] = v.FieldByName(destField.Name).Uint()
			case reflect.Float32, reflect.Float64:
				rowMap[destField.Name] = v.FieldByName(destField.Name).Float()
			case reflect.Bool:
				rowMap[destField.Name] = v.FieldByName(destField.Name).Bool()
			case reflect.Struct:
				rowMap[destField.Name] = v.FieldByName(destField.Name).Interface()
			default:
				rowMap[destField.Name] = v.FieldByName(destField.Name).String()
			}
		}
	}

	row := &bigquery.TableDataInsertAllRequestRows {
		Json: rowMap,
	}
	t.Rows = append(t.Rows, row)
}

func(t *TableData) InsertAll(client *http.Client, projectID, datasetID, tableID string) (*bigquery.TableDataInsertAllResponse, error) {
	// BigQueryの初期化
	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Println("bigquery.New")
		return nil, err
	}

	request := bigquery.TableDataInsertAllRequest(*t)
	resTablesData, err := bq.Tabledata.InsertAll(projectID, datasetID, tableID, &request).Do()

	return resTablesData, err
}