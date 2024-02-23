package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/microsoft/go-mssqldb"
	mssql "github.com/microsoft/go-mssqldb"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	csEnv := flag.String("csenv", "", "get sql connection string from this environment variable")
	csTmpl := flag.String("cs", "", "sql connection string")
	nullStrValue := flag.String("null", "NULL", "null string representation to use in CSV output")
	queryTimeoutSec := flag.Int("t", 60, "query timeout (seconds)")

	flag.Parse()

	connectionString := *csTmpl
	if connectionString == "" {
		// fetch name of environment variable:
		envName := *csEnv
		if envName == "" {
			_, _ = fmt.Fprintln(os.Stderr, "missing required sql connection string via -cs or -csenv flag")
			os.Exit(1)
		}

		connectionString = os.Getenv(envName)
		if connectionString == "" {
			_, _ = fmt.Fprintf(os.Stderr, "missing required sql connection string from environment variable '%s' (via -csenv flag)\n", envName)
			os.Exit(1)
		}
	}

	var err error
	var c *sql.DB
	if c, err = sql.Open(
		"sqlserver",
		connectionString,
	); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func(c *sql.DB) {
		_ = c.Close()
	}(c)

	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

		// test database connectivity with a quick Ping():
		if err = c.PingContext(ctx); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		cancel()
	}

	var text strings.Builder

	// read all query text from stdin:
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()

		// ready to execute?
		if strings.ToUpper(strings.TrimSpace(line)) == "GO" {
			// start a CSV writer:
			cw := csv.NewWriter(os.Stdout)

			// execute the query and write CSV output:
			q := &queryCSV{
				c:            c,
				cw:           cw,
				nullString:   *nullStrValue,
				queryTimeout: time.Second * time.Duration(*queryTimeoutSec),
			}
			err = q.execQuery(text.String())

			// make sure CSV flushes to stdout:
			cw.Flush()

			// handle any errors:
			if err != nil {
				var sqlErr mssql.Error
				if errors.As(err, &sqlErr) {
					// SQL server error:
					_, _ = fmt.Fprintf(os.Stderr, "%#v\n", sqlErr)
				} else {
					_, _ = fmt.Fprintln(os.Stderr, err)
				}
			}

			// prepare for next query:
			text.Reset()
		} else {
			// nope; append line to text:
			text.WriteString(line)
			text.WriteString("\r\n")
		}
	}

	if err = scanner.Err(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type csvWriter interface {
	Write(record []string) error
}

type queryCSV struct {
	c            *sql.DB
	cw           csvWriter
	nullString   string
	queryTimeout time.Duration
}

func (q *queryCSV) execQuery(text string) (err error) {
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
		return fmt.Errorf("error executing query: %w", err)
	}

nextResultSet:
	// separate result sets from each other (and from query) with empty lines:
	q.cw.Write(nil)

	var colTypes []*sql.ColumnType
	if colTypes, err = rows.ColumnTypes(); err != nil {
		return fmt.Errorf("error fetching column schema: %w", err)
	}

	if len(colTypes) > 0 {
		if err = q.writeResultSet(colTypes, rows); err != nil {
			return
		}
	}

	if rows.NextResultSet() {
		goto nextResultSet
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("error closing result set: %w", err)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error from result set: %w", err)
	}

	return
}

func (q *queryCSV) writeResultSet(colTypes []*sql.ColumnType, rows *sql.Rows) (err error) {
	// write the CSV header:
	colNames := q.writeHeader(colTypes)
	if err = q.cw.Write(colNames); err != nil {
		return fmt.Errorf("error writing CSV column header: %w", err)
	}

	formatted := make([]string, len(colTypes))
	rowValues := make([]any, len(colTypes))
	for rowCount := 0; rows.Next(); rowCount++ {
		// fetch column values:
		for i := range rowValues {
			rowValues[i] = new(any)
		}
		if err = rows.Scan(rowValues...); err != nil {
			return fmt.Errorf("error in row %d scanning: %w", rowCount+1, err)
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
					return fmt.Errorf("error in row %d constructing uuid from bytes: %w", rowCount+1, err)
				}
				formatted[i] = uv.String()
			case "DECIMAL":
			case "MONEY":
				formatted[i] = string(value.([]byte))
			case "BIT":
				if value.(bool) {
					formatted[i] = "1"
				} else {
					formatted[i] = "0"
				}
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
		if err = q.cw.Write(formatted); err != nil {
			return fmt.Errorf("error in row %d writing CSV: %w", rowCount+1, err)
		}
	}

	return
}

func (q *queryCSV) writeHeader(colTypes []*sql.ColumnType) (colNames []string) {
	colNames = make([]string, len(colTypes))

	// output column header including types:
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
			if length == 2147483645 || length == 1073741822 {
				sb.WriteString("max")
			} else {
				sb.WriteString(strconv.FormatInt(length, 10))
			}
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

	return
}
