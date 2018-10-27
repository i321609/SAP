package main

import (
	"database/sql"
	"flag"
	"log"
	"reflect"

	_ "SAP/go-hdb/driver"
)

//To run this use: go run lob_examples.go -dsn hdb://user:password@host:port
func main() {
	var connStr string
	//dsn is of the form: hdb://user:password@host:port?connectProp1=value&connectProp2=value
	flag.StringVar(&connStr, "dsn", "hdb://user:password@host:port", "database dsn")

	if !flag.Parsed() {
		flag.Parse()
	}

	//The name of the HANA driver is hdb
	db, err := sql.Open("hdb", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//Drop the table if it exists and ignore the errors
	_, _ = db.Exec("DROP TABLE TAB")

	//Create a table
	res, err := db.Exec("CREATE TABLE TAB (B BLOB, C CLOB)")
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := db.Prepare("INSERT INTO TAB VALUES(?, ?)")
	if err != nil {
		log.Fatal(err)
	}

	bArr := make([]byte, 100000)
	cArr := make([]byte, 100000)
	for i := 0; i < len(bArr); i++ {
		cArr[i] = 'A' + uint8(i%26)
		bArr[i] = uint8(i % 2)
	}

	res, err = stmt.Exec(bArr, cArr)
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d rows affected ", rowsAffected)

	var retBlob []byte
	var retClob []byte
	err = db.QueryRow("SELECT B, C FROM TAB").Scan(&retBlob, &retClob)
	if err != nil {
		log.Fatal(err)
	}

	if !reflect.DeepEqual(bArr, retBlob) {
		log.Printf("Unexpected blob value found")
	}
	if !reflect.DeepEqual(cArr, retClob) {
		log.Printf("Unexpected clob value found")
	}

	log.Printf("Successfully retrieved insert lob values")

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
}
