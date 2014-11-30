package gobq

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"

	"code.google.com/p/google-api-go-client/bigquery/v2"
)

type NicoVideo struct {
	Date int64 `json:"date"`
	No int64 `json:"no"`
	Vpos int64 `json:"vpos"`
	Comment string `json:"comment"`
	Command string `json:"command"`
}

const (
	// Google
	projectID    = "tksyokoyama"
	datasetID    = "chugokudb6sample"
	tableID      = "nicovideo"

)

func TestBigQuery(t *testing.T) {

	client, err := NewServiceAccountClient("328006125971-2h1ni3u1e0pobb7pqk2pqccq44dr7dae@developer.gserviceaccount.com",
							bigquery.BigqueryScope,
							"key.pem")
	if err != nil {
		t.Log("NewServiceAccountClient")
		t.Fatal(err)
	}

	// BigQueryの初期化
	bq, err := bigquery.New(client)
	if err != nil {
		t.Log("bigquery.New")
		t.Fatal(err)
	}

	// var tableSchema = &bigquery.TableSchema {
	// 		Fields: []*bigquery.TableFieldSchema{
	// 			&bigquery.TableFieldSchema{Name: "date", Type: "timestamp"},
	// 			&bigquery.TableFieldSchema{Name: "no", Type: "integer"},
	// 			&bigquery.TableFieldSchema{Name: "vpos", Type: "integer"},
	// 			&bigquery.TableFieldSchema{Name: "comment", Type: "string"},
	// 			&bigquery.TableFieldSchema{Name: "command", Type: "string"},
	// 			},
	// 	}

	// Bigquery側のテーブルの存在確認
	// _, err = bq.Tables.Get(projectID, datasetID, tableID).Do()
	var tables Tables
	_, err = tables.Get(client, projectID, datasetID, tableID)
	if err != nil {
		// テーブルがないので作る。
		t.Log("Table Not Found. start create table")

		// var newTable bigquery.Table
		// newTable.TableReference = &bigquery.TableReference{DatasetId: datasetID, ProjectId: projectID, TableId: tableID}
		// newTable.Schema = tableSchema

		// insRes, insErr := bq.Tables.Insert(projectID, datasetID, &newTable).Do()
		// if insErr != nil {
		// 	t.Log("tables.Insert")
		// 	t.Fatal(insErr)
		// }
		var nicoType NicoVideo
		insRes, insErr := tables.Insert(client, projectID, datasetID, tableID, nicoType)
		if insErr != nil {
			t.Log("tables.Insert")
			t.Fatal(insErr)
		}
		t.Log(insRes)
	}

	var request bigquery.TableDataInsertAllRequest

	f, err := os.Open("testdata/sm046.dat")
	if err != nil {
		t.Log("file.Open testdata/sm046.dat")
		t.Fatal(err)
	}
	defer f.Close()

	// 出力
	br := bufio.NewReader(f)

	for {
		line, _, err := br.ReadLine()
		if err != nil {
			t.Log("br.ReadLine")
			t.Log(err)
			break
		}

		var nico NicoVideo
		err = json.Unmarshal([]byte(line), &nico)
		if err != nil {
			t.Log(line)
			t.Fatalf("NicoVideo Format Error. %v \n", err)
		}

		rowMap := make(map[string]bigquery.JsonValue)
		rowMap["date"] = nico.Date
		rowMap["no"] = nico.No
		rowMap["vpos"] = nico.Vpos
		rowMap["comment"] = nico.Comment
		rowMap["command"] = nico.Command


		row := &bigquery.TableDataInsertAllRequestRows {
			Json: rowMap,
		}
		request.Rows = append(request.Rows, row)
	}

	resTablesData, err := bq.Tabledata.InsertAll(projectID, datasetID, tableID, &request).Do()
	if err != nil {
		t.Log("TablesData.InsertAll")
		for row := range request.Rows {
			t.Log(row)
		}
		t.Log(len(request.Rows))
		t.Fatal(err)
	}
	for errs := range resTablesData.InsertErrors {
		t.Log(errs)
	}

}