package mysqldump

import (
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestGetTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table_1").
		AddRow("Test_Table_2")

	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(rows)
}
