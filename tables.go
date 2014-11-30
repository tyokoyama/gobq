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

	ty := reflect.TypeOf(dest)
	for i := 0; i < ty.FieldAlign(); i++ {
		fmt.Println(ty.Field(i))
	}
	var tableSchema = &bigquery.TableSchema {
			Fields: []*bigquery.TableFieldSchema{
				&bigquery.TableFieldSchema{Name: "date", Type: "timestamp"},
				&bigquery.TableFieldSchema{Name: "no", Type: "integer"},
				&bigquery.TableFieldSchema{Name: "vpos", Type: "integer"},
				&bigquery.TableFieldSchema{Name: "comment", Type: "string"},
				&bigquery.TableFieldSchema{Name: "command", Type: "string"},
				},
		}

	// BigQueryの初期化
	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Println("bigquery.New")
		return nil, err
	}

	var newTable bigquery.Table
	newTable.TableReference = &bigquery.TableReference{DatasetId: datasetID, ProjectId: projectID, TableId: tableID}
	newTable.Schema = tableSchema

	insRes, err := bq.Tables.Insert(projectID, datasetID, &newTable).Do()
	if err != nil {
		fmt.Println("tables.Insert")
		return nil, err
	}

	return insRes, nil
}