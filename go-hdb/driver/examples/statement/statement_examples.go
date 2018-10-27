package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"time"
	"fmt"
	
	_ "SAP/go-hdb/driver"
)

//To run this use: go run statement_examples.go -dsn hdb://user:password@host:port
func main() {
	var connStr string
	//dsn is of the form: hdb://user:password@host:port?connectProp1=value&connectProp2=value
	flag.StringVar(&connStr, "dsn", "hdb://SYSTEM:Sybase123@10.58.180.202:30215?CURRENTSCHEMA=ORM_TEST", "database dsn")

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
	res, err := db.Exec("CREATE TABLE TAB (A DECIMAL(25, 4), B TIMESTAMP)")
	if err != nil {
		//log.Fatal(err)
		fmt.Printf("minz: after ddl %v\n", err)
	}

	//Please look at transaction_examples.go for examples on using a statement
	//within a transaction.
	//Prepare a statement once and it can be used multiple times
	stmt, err := db.Prepare("INSERT INTO TAB VALUES(?, ?)")
	if err != nil {
		log.Fatal(err)
	}

	//Now use the prepared statement created above to insert multiple values in the table
	res, err = stmt.Exec(2311.12345, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d rows affected ", rowsAffected)

	res, err = stmt.Exec(-332.2249, time.Date(2017, 11, 1, 18, 12, 22, 123456789, time.Local))
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d rows affected ", rowsAffected)

	//Set a timeout of 5 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err = stmt.ExecContext(ctx, 1.00129, time.Date(1998, 03, 01, 10, 03, 11, 0, time.Local))
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d rows affected ", rowsAffected)

	//Once done with the prepared statement close it
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	//Set a timeout of 8 seconds
	ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	queryStmt, err := db.Prepare("SELECT * FROM TAB")
	rows, err := queryStmt.QueryContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	//Now iterate over the returned rows to get the returned values
	var retDec string
	var retTs time.Time
	for {
		if !rows.Next() {
			break
		}
		err = rows.Scan(&retDec, &retTs)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Value is: %s %s", retDec, retTs)
	}

	//Close the rows when done
	err = rows.Close()
	if err != nil {
		log.Fatal(err)
	}

	//Get the first row in the result set
	err = queryStmt.QueryRow().Scan(&retDec, &retTs)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Value is: %s %s", retDec, retTs)

	//Close the statement when done
	err = queryStmt.Close()
	if err != nil {
		log.Fatal(err)
	}
}
