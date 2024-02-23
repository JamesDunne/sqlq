package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/microsoft/go-mssqldb"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	csTmpl := flag.String("cs", "", "sql connection string")
	nullStrValue := flag.String("null", "NULL", "null string representation to use in CSV output")
	queryTimeoutSec := flag.Int("t", 60, "query timeout (seconds)")

	flag.Parse()

	var driverName string
	var connectionString string

	driverName = "sqlserver"
	connectionString = *csTmpl
	if connectionString == "" {
		fmt.Fprintln(os.Stderr, "missing required sql connection string flag -cs")
		os.Exit(1)
	}

	var err error
	var c *sql.DB
	if c, err = sql.Open(
		driverName,
		connectionString,
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()

	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

		// test database connectivity with a quick Ping():
		if err = c.PingContext(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		cancel()
	}

	// read all query text from stdin until EOF:
	var text []byte
	text, err = io.ReadAll(os.Stdin)

	// start a CSV writer:
	cw := csv.NewWriter(os.Stdout)

	// execute the query and write CSV output:
	err = (&QueryCSV{
		c:            c,
		cw:           cw,
		nullString:   *nullStrValue,
		queryTimeout: time.Second * time.Duration(*queryTimeoutSec),
	}).execQuery(string(text))

	// make sure CSV flushes to stdout:
	cw.Flush()

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	os.Stdout.Sync()
	os.Stderr.Sync()
}

type QueryCSV struct {
	c            *sql.DB
	cw           *csv.Writer
	nullString   string
	queryTimeout time.Duration
}

func (q *QueryCSV) execQuery(text string) (err error) {
	var rows *sql.Rows

	ctx, cancel := context.WithTimeout(context.Background(), q.queryTimeout)
	defer cancel()

	tStart := time.Now()
	rows, err = q.c.QueryContext(
		ctx,
		text,
	)
	tEnd := time.Now()

	_, _ = tStart, tEnd

	if err != nil {
		return
	}

	var colTypes []*sql.ColumnType
	if colTypes, err = rows.ColumnTypes(); err != nil {
		return
	}

	cw := q.cw

	colNames := make([]string, len(colTypes))
	for i := range colTypes {
		nullable, hasNullable := colTypes[i].Nullable()
		length, hasLength := colTypes[i].Length()
		decimalSize, decimalScale, hasDecimalSize := colTypes[i].DecimalSize()

		sb := strings.Builder{}
		colName := colTypes[i].Name()
		/*if colName != ""*/ {
			sb.WriteRune('[')
			sb.WriteString(strings.ReplaceAll(colName, "]", "]]"))
			sb.WriteRune(']')
			sb.WriteRune(' ')
		}
		sb.WriteString(colTypes[i].DatabaseTypeName())
		if hasLength {
			sb.WriteRune('(')
			sb.WriteString(strconv.FormatInt(length, 10))
			sb.WriteRune(')')
		} else if hasDecimalSize {
			sb.WriteRune('(')
			sb.WriteString(strconv.FormatInt(decimalSize, 10))
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatInt(decimalScale, 10))
			sb.WriteRune(')')
		}
		if hasNullable {
			sb.WriteRune(' ')
			if !nullable {
				sb.WriteString("NOT ")
			}
			sb.WriteString("NULL")
		}
		colNames[i] = sb.String()
	}

	cw.Write(colNames)

	formatted := make([]string, len(colNames))
	rowValues := make([]any, len(colNames))
	for rowCounter := 0; rows.Next(); rowCounter++ {
		// fetch column values:
		for i := range rowValues {
			rowValues[i] = new(any)
		}
		if err = rows.Scan(rowValues...); err != nil {
			return
		}

		// format column values for output to CSV:
		for i := range rowValues {
			value := *rowValues[i].(*any)
			if value == nil {
				formatted[i] = q.nullString
				continue
			}

			// specialize formatting based on type:
			switch colTypes[i].DatabaseTypeName() {
			case "UNIQUEIDENTIFIER":
				var uv uuid.UUID
				uv, err = uuid.FromBytes(value.([]byte))
				if err != nil {
					return
				}
				formatted[i] = uv.String()
			default:
				switch v := value.(type) {
				case []byte:
					formatted[i] = "0x" + hex.EncodeToString(v)
				default:
					formatted[i] = fmt.Sprintf("%v", v)
				}
			}
		}

		// write the CSV line:
		if err = cw.Write(formatted); err != nil {
			return
		}

		// make sure CSV flushes to stdout:
		cw.Flush()

		if err = cw.Error(); err != nil {
			return
		}
	}

	if err = rows.Close(); err != nil {
		return
	}

	if err = rows.Err(); err != nil {
		return
	}

	return
}
