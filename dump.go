package mysqldump

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"
	"time"
)

type table struct {
	Name   string
	SQL    string
	Values string
}

type dump struct {
	DumpVersion   string
	ServerVersion string
	Tables        []*table
	CompleteTime  string
	WithSQLValue  bool
}

const (
	version = "v0.0.1"
	tmpl    = `-- Go SQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version  {{ .ServerVersion  }}

{{ range .Tables }}
--
-- Table structure for table {{ .Name  }}
--
DROP TABLE IF EXISTS {{ .Name }};
{{ .SQL }};
`

	tmplSQLValue = `
--
-- Dumping data for table {{ .Name }}
--
LOCK TABLES {{ .Name }} WRITE;
{{ if .Values }}
INSERT INTO {{ .Name }} VALUES {{ .Values }};
{{ end }}
UNLOCK TABLES;
`
	complete = `	
{{ end }}
-- Dump completed on {{ .CompleteTime }}
`
)

func (d *Dumper) Dump() (string, error) {
	name := time.Now().Format(d.format)
	p := path.Join(d.dir, name+".sql")

	if e, _ := exists(p); e {
		return p, fmt.Errorf("Dump '" + name + "' already exists.")
	}

	// create .sql file
	f, err := os.Create(p)
	if err != nil {
		return p, err
	}
	defer f.Close()

	data := dump{
		DumpVersion:  version,
		Tables:       make([]*table, 0),
		WithSQLValue: d.withSQLValue,
	}

	// get server sversion
	if data.ServerVersion, err = getServerVersion(d.db); err != nil {
		return p, err
	}

	// get tables
	tables, err := getTables(d.db)
	if err != nil {
		return p, err
	}

	// get sql for each table
	for _, name := range tables {
		if t, err := createTable(d.db, name, d.withSQLValue); err == nil {
			data.Tables = append(data.Tables, t)
		} else {
			return p, err
		}
	}

	// set complete time
	data.CompleteTime = time.Now().String()

	// write dump to file
	t, err := template.New("mysqldump").Parse(d.genTemplate())
	if err != nil {
		return p, err
	}

	if err = t.Execute(f, data); err != nil {
		return p, err
	}

	return p, nil
}

func (d *Dumper) genTemplate() string {
	template := tmpl + complete
	if d.withSQLValue {
		template = tmpl + tmplSQLValue + complete
	}

	return template
}

func getTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)

	// Get table list
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}

	defer rows.Close()

	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}

	return tables, rows.Err()
}

func getServerVersion(db *sql.DB) (string, error) {
	var serverVersion sql.NullString
	if err := db.QueryRow("SELECT version()").Scan(&serverVersion); err != nil {
		return "", err
	}
	return serverVersion.String, nil
}

func createTable(db *sql.DB, name string, withValue bool) (*table, error) {
	var err error
	t := &table{Name: name}

	if t.SQL, err = createTableSQL(db, name); err != nil {
		return nil, err
	}

	if withValue {
		if t.Values, err = createTableValues(db, name); err != nil {
			return nil, err
		}
	}

	return t, nil
}

func createTableSQL(db *sql.DB, name string) (string, error) {
	var tableName sql.NullString
	var tableSql sql.NullString
	var charsetClient sql.NullString
	var collation sql.NullString

	err := db.QueryRow("SHOW CREATE TABLE "+name).Scan(&tableName, &tableSql)
	err2 := db.QueryRow("SHOW CREATE TABLE "+name).Scan(&tableName, &tableSql, &charsetClient, &collation)
	if err != nil && err2 != nil {
		return "", fmt.Errorf("%+v %+v", err, err2)
	}

	if tableName.String != name {
		return "", fmt.Errorf("returned table is not the same as requested table")
	}

	return tableSql.String, nil
}

func createTableValues(db *sql.DB, name string) (string, error) {
	rows, err := db.Query("SELECT * FROM " + name)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("No columns in table " + name + ".")
	}

	dataText := make([]string, 0)
	for rows.Next() {
		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))

		for i := range data {
			ptrs[i] = &data[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = "'" + value.String + "'"
			} else {
				dataStrings[key] = "null"
			}
		}
		dataText = append(dataText, "("+strings.Join(dataStrings, ",")+")")
	}

	return strings.Join(dataText, ","), rows.Err()
}
