package mysqldump

import (
	"database/sql"
	"fmt"
	"os"
)

type Dumper struct {
	db           *sql.DB
	format       string
	dir          string
	withSQLValue bool
}

// Register creates a new dumper.
// db: database that will be dumped https://pkg.go.dev/database/sql#DB
// dir: path to the directory where the dumps will be stored.
// format: format to be used to name eache dump file. Uses time.Time format https://pkg.go.dev/time#Time.Format
func Register(db *sql.DB, dir, format string, withValue bool) (*Dumper, error) {
	if !isDir(dir) {
		return nil, fmt.Errorf("Invalid directory")
	}

	return &Dumper{
		db:           db,
		format:       format,
		dir:          dir,
		withSQLValue: withValue,
	}, nil
}

// Close closes the dumper.
func (d *Dumper) Close() error {
	defer func() {
		d.db = nil
	}()

	return d.db.Close()
}

func exists(p string) (bool, os.FileInfo) {
	f, err := os.Open(p)
	if err != nil {
		return false, nil
	}

	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return false, nil
	}

	return true, fi
}

func isFile(p string) bool {
	if e, fi := exists(p); e {
		return fi.Mode().IsRegular()
	}

	return false
}

func isDir(p string) bool {
	if e, fi := exists(p); e {
		return fi.Mode().IsDir()
	}
	return false
}
