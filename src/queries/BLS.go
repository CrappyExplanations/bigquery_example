package queries

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Bls struct {
	Project string
	DataPath string
}

type BlsData struct {
	SeriesID      string               `bigquery:"series_id"`
	Year          bigquery.NullInt64   `bigquery:"year"`
	Period        bigquery.NullString  `bigquery:"period"`
	Value         bigquery.NullFloat64 `bigquery:"value"`
	FootnoteCodes bigquery.NullString  `bigquery:"footnote_codes"`
	Date          bigquery.NullDate    `bigquery:"date"`
	SeriesTitle   bigquery.NullString  `bigquery:"series_title"`
}

func NewBLS(dataPath string) Bls {
	proj := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if proj == "" {
		fmt.Println("GOOGLE_CLOUD_PROJECT environment variable must be set.")
		os.Exit(1)
	}
	b := Bls{proj, dataPath}
	return b
}

func (b *Bls) Query(year string) (*bigquery.RowIterator, error) {
	//validate that we have an actual number provided for the year
	if  _, err := strconv.Atoi(year); err != nil {
		return nil, errors.New("Invalid year: " + year)
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, b.Project)
	if err != nil {
		return nil, err
	}

	query := client.Query(`SELECT * FROM ` +
		"`bigquery-public-data.bls.unemployment_cps`" +
		`WHERE year = ` + year + `;`)

	return query.Read(ctx)
}

func (b *Bls) QueryAndStore(w io.Writer, year string) error {
	rowsItr, err := b.Query(year)

	if err != nil {
		return err
	}

	for {
		var row BlsData
		err = rowsItr.Next(&row)
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		j, err := json.Marshal(row)
		if err != nil {
			return err
		}
		w.Write(j)
		//newline not really needed but makes output easier to read
		w.Write([]byte("\n"))
	}
}
