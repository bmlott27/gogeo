package main

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bmlott27/gogeo/postgres"
	"github.com/bmlott27/gogeo/utilities"
	_ "github.com/lib/pq"
)

const (
	GC_PROJECT = "glossy-chimera-366014"
	GC_DATASET = "gogeo"
	GC_TABLE   = "al_counties"
)

// Item represents a row item.
type Item struct {
	Id       int
	CountyFP string
	Geom     string
}

// Save implements the ValueSaver interface.
// This example disables best-effort de-duplication, which allows for higher throughput.
func (i *Item) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"id":        i.Id,
		"county_fp": i.CountyFP,
		"geom":      i.Geom,
	}, bigquery.NoDedupeID, nil
}

// Main function
func main() {

	// create a table in BigQuery to
	// hold our data
	err := createTable(GC_PROJECT, GC_DATASET, GC_TABLE)

	// check errors
	utilities.CheckErr(err)

	// attach to the local database
	db := postgres.Connect()

	// query the table in the Postgre database
	rows, err := db.Query("SELECT id, \"COUNTYFP\", ST_AsText(geom) FROM al_counties_wgs84")

	// check errors
	utilities.CheckErr(err)

	items := []*Item{}

	// loop through the rows and create a
	// list of items to add to the BigQuery table
	for rows.Next() {
		var id int
		var countyFP string
		var wkt string

		err = rows.Scan(&id, &countyFP, &wkt)

		// check errors
		utilities.CheckErr(err)

		items = append(items, &Item{Id: id, CountyFP: countyFP, Geom: wkt})
		if len(items) == 5 {
			for err = insertRows(GC_PROJECT, GC_DATASET, GC_TABLE, items); err != nil; {
				utilities.CheckErr(err)
				time.Sleep(2 * time.Second)
			}

			items = []*Item{}
		}
	}

	// insert the items into the table
	if len(items) > 0 {
		err = insertRows(GC_PROJECT, GC_DATASET, GC_TABLE, items)

		// check errors
		utilities.CheckErr(err)
	}

}

func createTable(projectID, datasetID, tableID string) error {

	// attach to BigQuery client
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)

	// check for errors
	utilities.CheckErr(err)

	defer client.Close()

	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "county_fp", Type: bigquery.StringFieldType},
		{Name: "geom", Type: bigquery.GeographyFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         schema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, metaData); err != nil {
		return err
	}

	return nil
}

// insertRows demonstrates inserting data into a table using the streaming insert mechanism.
func insertRows(projectID, datasetID, tableID string, items []*Item) error {
	// attach to BigQuery client
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)

	// check for errors
	utilities.CheckErr(err)

	defer client.Close()

	inserter := client.Dataset(datasetID).Table(tableID).Inserter()
	if err := inserter.Put(ctx, items); err != nil {
		return err
	}
	return nil
}
