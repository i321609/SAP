package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"time"

	_ "SAP/go-hdb/driver"
)

//To run this use: go run transaction_examples.go -dsn hdb://user:password@host:port
func main() {
	var connStr string
	//dsn is of the form: hdb://user:password@host:port?connectProp1=value&connectProp2=value
	flag.StringVar(&connStr, "dsn", "hdb://SYSTEM:Sybase123@10.58.180.202:30215", "database dsn")

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
	res, err := db.Exec("CREATE TABLE TAB (A INT, B NVARCHAR(5000))")
	if err != nil {
		log.Fatal(err)
	}

	//Now start a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	//Prepare a statement once and it can be used multiple times
	stmt, err := tx.Prepare("INSERT INTO TAB VALUES(?, ?)")
	if err != nil {
		log.Fatal(err)
	}

	//Now use the prepared statement created above to insert multiple values in the table
	res, err = stmt.Exec(12, "Waterloo")
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d rows affected ", rowsAffected)

	res, err = stmt.Exec(-23212, "Kitchener")
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d rows affected ", rowsAffected)

	//Set a timeout of 3 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err = stmt.ExecContext(ctx, 455, "CAMBRIDGE")
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

	//Use the prepared statement above to query the database multiple times
	queryStmt, err := tx.Prepare("SELECT B FROM TAB WHERE A<=?")
	rows, err := queryStmt.Query(20)
	if err != nil {
		log.Fatal(err)
	}

	//Now iterate over the returned rows to get the returned values
	var retVal string
	for {
		if !rows.Next() {
			break
		}
		err = rows.Scan(&retVal)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Value is: %s", retVal)
	}

	//Close the rows when done
	err = rows.Close()
	if err != nil {
		log.Fatal(err)
	}

	//Get the first row in the result set
	err = queryStmt.QueryRow(0).Scan(&retVal)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Value is: %s", retVal)

	//Close the statement when done
	err = queryStmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	//You can also use non-prepared statements in transactions
	res, err = tx.Exec("INSERT INTO TAB VALUES(1,'MONTREAL')")
	if err != nil {
		log.Fatal(err)
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d rows affected ", rowsAffected)

	var rowCount int
	err = tx.QueryRow("SELECT COUNT(*) FROM TAB").Scan(&rowCount)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d is the row count of the table", rowCount)

	//Commit or rollback the transaction to end it
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Commit successful")

	//You can also specify the isolation level to be used for the transaction
	options := sql.TxOptions{}
	//Isolation level 3 is serializable in HANA
	options.Isolation = sql.IsolationLevel(2)
	ctx = context.Background()
	tx, err = db.BeginTx(ctx, &options)

	isolationLevel := ""
	//You can find the isolation level in HANA using this query
	err = tx.QueryRow("SELECT ISOLATION_LEVEL FROM M_TRANSACTIONS WHERE CONNECTION_ID=CURRENT_CONNECTION").Scan(&isolationLevel)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("The isolation level is %s ", isolationLevel)

	err = tx.Rollback()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Rollback successful")
}
