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

	// Bigquery側のテーブルの存在確認
	var tables Tables
	_, err = tables.Get(client, projectID, datasetID, tableID)
	if err != nil {
		// テーブルがないので作る。
		t.Log("Table Not Found. start create table")

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

	tabledata := new(TableData)

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

		tabledata.Add(nico)
	}

	resTablesData, err := tabledata.InsertAll(client, projectID, datasetID, tableID)
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