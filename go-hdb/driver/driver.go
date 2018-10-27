package driver

/*
#cgo CFLAGS: -I${SRCDIR}/includes
#cgo LDFLAGS: -Wl,-rpath -Wl,\$ORIGIN
#include <stddef.h>
#include <stdlib.h>
#include "DBCAPI_DLL.h"
#include "DBCAPI.h"
*/
import "C"
import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"
	"unsafe"
)

//DriverName is the driver name to use with sql.Open for hdb databases.
const DriverName = "hdb"

//For conversion between string and timestamp
const TS_LAYOUT_STRING = "2006-01-02 15:04:05.000000000"

//For conversion between string and date
const DATE_LAYOUT_STRING = "2006-01-02"

//For conversion between string and time
const TIME_LAYOUT_STRING = "15:04:05"

//Max length of varbinary column
const MAX_BINARY_SIZE = 5000

const DBCAPI_DATA_VALUE_STRUCT_LEN = 48

const DBCAPI_COLUMN_INFO_STRUCT_LEN = 112

const DBCAPI_BIND_DATA_STRUCT_LEN = 64

const DEFAULT_ISOLATION_LEVEL = 1

const (
	FunctionCode_DDL = 1
)

type drv struct{}

//database connection
type connection struct {
	conn *C.dbcapi_connection
}

type transaction struct {
	conn *C.dbcapi_connection
}

//statement
type statement struct {
	query string
	stmt  *C.dbcapi_stmt
	conn  *C.dbcapi_connection
	Ok    bool
}

//result set
type rows struct {
	stmt *C.dbcapi_stmt
	conn *C.dbcapi_connection
}

//keep track of rows affected after inserts and updates
type result struct {
	stmt *C.dbcapi_stmt
	conn *C.dbcapi_connection
}

//needed to handle nil time values
type NullTime struct {
	Time  time.Time
	Valid bool
}

//needed to handle nil binary values
type NullBytes struct {
	Bytes []byte
	Valid bool
}

func init() {
	sql.Register(DriverName, &drv{})
	cName := C.CString("hdbGolangDriver")
	defer C.free(unsafe.Pointer(cName))
	api := C.dbcapi_init(cName, C.DBCAPI_API_VERSION_1, nil)
	if api != 1 {
		fmt.Println("Loading the dbcapi library has failed, please ensure that it is on your library path")
		return
	}
}

func getError(conn *C.dbcapi_connection) error {
	if conn == nil {
		return driver.ErrBadConn
	}

	len := C.size_t(C.DBCAPI_ERROR_SIZE)
	err := make([]byte, len)
	errCode := C.dbcapi_error(conn, (*C.char)(unsafe.Pointer(&err[0])), len)
	//-10807: connection lost and -10108: reconnect error and -10821 session not connected
	if errCode == -10807 || errCode == -10108 || errCode == -10821 {
		return driver.ErrBadConn
	}
	errorLen := C.dbcapi_error_length(conn)
	return errors.New(fmt.Sprintf("%d: %s", errCode, string(err[:errorLen-1])))
}

func isBadConnection(conn *C.dbcapi_connection) bool {
	if conn == nil {
		return true
	}

	len := C.size_t(C.DBCAPI_ERROR_SIZE)
	err := make([]byte, len)
	errCode := C.dbcapi_error(conn, (*C.char)(unsafe.Pointer(&err[0])), len)
	//-10807: connection lost and -10108: reconnect error and -10821 session not connected
	return errCode == -10807 || errCode == -10108 || errCode == -10821
}

func (d *drv) Open(dsn string) (driver.Conn, error) {
	dsnInfo, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	conn := C.dbcapi_new_connection()
	if conn == nil {
		return nil, errors.New("dbcapi failed to allocate a connection object")
	}

	var connectString bytes.Buffer
	connectString.WriteString("ServerNode=")
	connectString.WriteString(dsnInfo.Host)
	connectString.WriteString(";")
	connectString.WriteString("UID=")
	connectString.WriteString(dsnInfo.Username)
	connectString.WriteString(";")
	connectString.WriteString("PWD=")
	connectString.WriteString(dsnInfo.Password)
	connectString.WriteString(";")

	connProps := dsnInfo.ConnectProps
	for key := range connProps {
		values := connProps[key]
		if len(values) > 0 {
			connectString.WriteString(key)
			connectString.WriteString("=")
			connectString.WriteString(values[0])
			connectString.WriteString(";")
		}
	}

	connStr := C.CString(connectString.String())
	defer C.free(unsafe.Pointer(connStr))
	connRes := C.dbcapi_connect(conn, connStr)
	if connRes != 1 {
		return nil, getError(conn)
	}
	autoCommRes := C.dbcapi_set_autocommit(conn, 1)
	if autoCommRes != 1 {
		return nil, getError(conn)
	}
	return &connection{conn: conn}, nil
}

func (connection *connection) Prepare(query string) (driver.Stmt, error) {
	psql := C.CString(query)
	defer C.free(unsafe.Pointer(psql))
	ps := C.dbcapi_prepare(connection.conn, psql)

	if ps == nil {
		return nil, getError(connection.conn)
	}

	return &statement{query: query, stmt: ps, conn: connection.conn}, nil
}

func (connection *connection) Close() error {
	disconn := C.dbcapi_disconnect(connection.conn)
	var err error
	if disconn != 1 {
		err = getError(connection.conn)
	}
	C.dbcapi_free_connection(connection.conn)
	return err
}

func (connection *connection) Begin() (driver.Tx, error) {
	autoCommRes := C.dbcapi_set_autocommit(connection.conn, 0)
	if autoCommRes != 1 {
		return nil, getError(connection.conn)
	}

	return &transaction{conn: connection.conn}, nil
}

func (connection *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, errors.New("Setting of read only option is currently unsupported")
	}

	isolationLevel := int(opts.Isolation)
	//0 means that use the driver's default isolation level
	if isolationLevel == 0 {
		return connection.Begin()
	}
	if isolationLevel < 0 || isolationLevel > 3 {
		return nil, errors.New(fmt.Sprintf("Unsupported isolation level requested: %d ", isolationLevel))
	}
	err := setIsolationLevel(connection.conn, isolationLevel)
	if err != nil {
		return nil, err
	}

	return connection.Begin()
}

func setIsolationLevel(conn *C.dbcapi_connection, isolationLevel int) error {
	isolationLevelRes := C.dbcapi_set_transaction_isolation(conn, C.dbcapi_u32(isolationLevel))
	if isolationLevelRes != 1 {
		return getError(conn)
	}

	return nil
}

func (connection *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	if args != nil && len(args) != 0 {
		return nil, driver.ErrSkip
	}

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))
	stmt := C.dbcapi_execute_direct(connection.conn, sql)

	if stmt == nil {
		return nil, getError(connection.conn)
	}

	funcCode := C.dbcapi_get_function_code(stmt)
	if funcCode == FunctionCode_DDL {
		//return nil,nil minz
		return driver.ResultNoRows, nil
	}

	return &result{stmt: stmt, conn: connection.conn}, nil
}

func (connection *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	if args != nil && len(args) != 0 {
		return nil, driver.ErrSkip
	}

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))
	stmt := C.dbcapi_execute_direct(connection.conn, sql)

	if stmt == nil {
		return nil, getError(connection.conn)
	}

	return &rows{stmt: stmt, conn: connection.conn}, nil
}

func (rows *rows) Close() error {
	closeRowsRes := C.dbcapi_reset(rows.stmt)
	if closeRowsRes != 1 {
		return getError(rows.conn)
	}
	return nil
}

func (rows *rows) Columns() []string {
	colCount := int(C.dbcapi_num_cols(rows.stmt))
	if colCount == -1 {
		return nil
	}
	columnNames := make([]string, colCount)
	for i := 0; i < colCount; i++ {
		colInfo := (*C.struct_dbcapi_column_info)(C.malloc(DBCAPI_COLUMN_INFO_STRUCT_LEN))
		defer C.free(unsafe.Pointer(colInfo))
		colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(i), colInfo)
		columnNames[i] = ""
		if colinfoRes == 1 {
			columnNames[i] = C.GoString(colInfo.name)
		}
	}

	return columnNames
}

func (rows *rows) ColumnTypeDatabaseTypeName(index int) string {
	dbTypeName := ""
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(DBCAPI_COLUMN_INFO_STRUCT_LEN))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)
	if colinfoRes == 1 {
		dbTypeName = getNativeType(int(colInfo.native_type))
	}

	return dbTypeName
}

func (rows *rows) ColumnTypeNullable(index int) (bool, bool) {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(DBCAPI_COLUMN_INFO_STRUCT_LEN))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)

	nullable := false
	ok := false
	if colinfoRes == 1 {
		ok = true
		if colInfo.nullable == 1 {
			nullable = true
		}
	}

	return nullable, ok
}

func (rows *rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(DBCAPI_COLUMN_INFO_STRUCT_LEN))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)

	precision := int64(0)
	scale := int64(0)
	ok := false
	if colinfoRes == 1 && colInfo.native_type == C.DT_DECIMAL {
		precision = int64(colInfo.precision)
		scale = int64(colInfo.scale)
		ok = true
	}

	return precision, scale, ok
}

func (rows *rows) ColumnTypeLength(index int) (int64, bool) {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(DBCAPI_COLUMN_INFO_STRUCT_LEN))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)

	length := int64(0)
	ok := false
	if colinfoRes == 1 {
		dbcapiType := colInfo._type
		if dbcapiType == C.A_BINARY {
			length = int64(colInfo.max_size)
			ok = true
		} else if dbcapiType == C.A_STRING {
			hanaType := colInfo.native_type
			if hanaType != C.DT_DATE && hanaType != C.DT_TIME && hanaType != C.DT_TIMESTAMP && hanaType != C.DT_DECIMAL &&
				hanaType != C.DT_DAYDATE && hanaType != C.DT_SECONDTIME &&
				hanaType != C.DT_LONGDATE && hanaType != C.DT_SECONDDATE {
				length = int64(colInfo.max_size)
				ok = true
			}
		}
	}

	return length, ok
}

func (rows *rows) ColumnTypeScanType(index int) reflect.Type {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(DBCAPI_COLUMN_INFO_STRUCT_LEN))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)
	defer C.free(unsafe.Pointer(colInfo))

	if colinfoRes != 1 {
		return reflect.TypeOf(string(""))
	}

	dbcapiType := colInfo._type
	switch dbcapiType {
	case C.A_BINARY:
		return reflect.TypeOf([]byte{0})
	case C.A_STRING:
		hanaType := colInfo.native_type
		if hanaType == C.DT_DATE || hanaType == C.DT_TIME || hanaType == C.DT_TIMESTAMP ||
			hanaType == C.DT_DAYDATE || hanaType == C.DT_SECONDTIME ||
			hanaType == C.DT_LONGDATE || hanaType == C.DT_SECONDDATE {
			return reflect.TypeOf(time.Time(time.Time{}))
		}
		return reflect.TypeOf(string(""))
	case C.A_DOUBLE:
		return reflect.TypeOf(float64(0.0))
	case C.A_VAL64:
		return reflect.TypeOf(int64(0))
	case C.A_UVAL64:
		return reflect.TypeOf(uint64(0))
	case C.A_VAL32:
		return reflect.TypeOf(int32(0))
	case C.A_UVAL32:
		return reflect.TypeOf(uint32(0))
	case C.A_VAL16:
		return reflect.TypeOf(int16(0))
	case C.A_UVAL16:
		return reflect.TypeOf(uint16(0))
	case C.A_VAL8:
		return reflect.TypeOf(int8(0))
	case C.A_UVAL8:
		return reflect.TypeOf(uint8(0))
	case C.A_FLOAT:
		return reflect.TypeOf(float32(0.0))
	default:
		return reflect.TypeOf(string(""))
	}
}

func getNativeType(nativeTypeCode int) string {
	switch nativeTypeCode {
	case C.DT_DATE:
		return "DATE"
	case C.DT_DAYDATE:
		return "DATE"
	case C.DT_TIME:
		return "TIME"
	case C.DT_SECONDTIME:
		return "TIME"
	case C.DT_TIMESTAMP:
		return "TIMESTAMP"
	case C.DT_LONGDATE:
		return "TIMESTAMP"
	case C.DT_SECONDDATE:
		return "TIMESTAMP"
	case C.DT_VARCHAR1:
		return "VARCHAR"
	case C.DT_VARCHAR2:
		return "VARCHAR"
	case C.DT_ALPHANUM:
		return "ALPHANUM"
	case C.DT_CHAR:
		return "CHAR"
	case C.DT_CLOB:
		return "CLOB"
	case C.DT_STRING:
		return "STRING"
	case C.DT_DOUBLE:
		return "DOUBLE"
	case C.DT_REAL:
		return "REAL"
	case C.DT_DECIMAL:
		return "DECIMAL"
	case C.DT_INT:
		return "INT"
	case C.DT_SMALLINT:
		return "SMALLINT"
	case C.DT_BINARY:
		return "BINARY"
	case C.DT_VARBINARY:
		return "VARBINARY"
	case C.DT_BSTRING:
		return "BSTRING"
	case C.DT_BLOB:
		return "BLOB"
	case C.DT_ST_GEOMETRY:
		return "ST_GEOMETRY"
	case C.DT_ST_POINT:
		return "ST_POINT"
	case C.DT_TINYINT:
		return "TINYINT"
	case C.DT_BIGINT:
		return "BIGINT"
	case C.DT_BOOLEAN:
		return "BOOLEAN"
	case C.DT_NSTRING:
		return "NSTRING"
	case C.DT_SHORTTEXT:
		return "SHORTTEXT"
	case C.DT_NCHAR:
		return "NCHAR"
	case C.DT_NVARCHAR:
		return "NVARCHAR"
	case C.DT_NCLOB:
		return "NCLOB"
	case C.DT_TEXT:
		return "TEXT"
	case C.DT_BINTEXT:
		return "BINTEXT"
	default:
		return "UNKNOWN"
	}
}

func (rows *rows) Next(dest []driver.Value) error {
	fetchNextRes := C.dbcapi_fetch_next(rows.stmt)
	if fetchNextRes != 1 {
		if isBadConnection(rows.conn) {
			return driver.ErrBadConn
		}
		return io.EOF
	}

	return rows.getNextRowData(dest)
}

func (rows *rows) HasNextResultSet() bool {
	return true
}

func (rows *rows) NextResultSet() error {
	nextRs := C.dbcapi_get_next_result(rows.stmt)
	if nextRs != 1 {
		return io.EOF
	}

	return nil
}

func (rows *rows) getNextRowData(dest []driver.Value) error {
	for i := 0; i < len(dest); i++ {
		col := (*C.struct_dbcapi_data_value)(C.malloc(DBCAPI_DATA_VALUE_STRUCT_LEN))
		defer C.free(unsafe.Pointer(col))

		getCol := C.dbcapi_get_column(rows.stmt, C.dbcapi_u32(i), col)
		if getCol != 1 {
			return getError(rows.conn)
		}

		err := getData(col, &dest[i], i, rows.stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func getData(col *C.struct_dbcapi_data_value, val *driver.Value, index int, stmt *C.dbcapi_stmt) error {
	if col.is_null != nil && *col.is_null == 1 {
		*val = nil
		return nil
	}

	colType := col._type
	switch colType {
	case C.A_BINARY:
		bArr := make([]byte, int(*col.length))
		if *col.length <= MAX_BINARY_SIZE {
			copy(bArr, (*[MAX_BINARY_SIZE]byte)(unsafe.Pointer(col.buffer))[:])
			*val = bArr
		} else {
			*val = convertToLargeByteArray(col.buffer, uint(*col.length))
		}

	case C.A_STRING:
		strPtr := (*C.char)(unsafe.Pointer(col.buffer))
		strVal := C.GoString(strPtr)
		colInfo := (*C.struct_dbcapi_column_info)(C.malloc(DBCAPI_COLUMN_INFO_STRUCT_LEN))
		defer C.free(unsafe.Pointer(colInfo))
		colinfoRes := C.dbcapi_get_column_info(stmt, C.dbcapi_u32(index), colInfo)
		if colinfoRes != 1 {
			return errors.New(fmt.Sprintf("Could not determine the column type for the: %d column", index+1))
		}

		hanaType := colInfo.native_type
		if hanaType == C.DT_TIMESTAMP || hanaType == C.DT_LONGDATE || hanaType == C.DT_SECONDDATE {
			tsVal, err := time.Parse(TS_LAYOUT_STRING, strVal)
			if err != nil {
				return err
			}
			*val = tsVal
		} else if hanaType == C.DT_TIME || hanaType == C.DT_SECONDTIME {
			tVal, err := time.Parse(TIME_LAYOUT_STRING, strVal)
			if err != nil {
				return err
			}
			*val = tVal
		} else if hanaType == C.DT_DATE || hanaType == C.DT_DAYDATE {
			dVal, err := time.Parse(DATE_LAYOUT_STRING, strVal)
			if err != nil {
				return err
			}
			*val = dVal
		} else {
			*val = strVal
		}
	case C.A_DOUBLE:
		doublePtr := (*float64)(unsafe.Pointer(col.buffer))
		*val = *doublePtr
	case C.A_VAL64:
		longPtr := (*int64)(unsafe.Pointer(col.buffer))
		*val = *longPtr
	case C.A_UVAL64:
		longPtr := (*uint64)(unsafe.Pointer(col.buffer))
		*val = *longPtr
	case C.A_VAL32:
		intPtr := (*int32)(unsafe.Pointer(col.buffer))
		*val = *intPtr
	case C.A_UVAL32:
		intPtr := (*uint32)(unsafe.Pointer(col.buffer))
		*val = *intPtr
	case C.A_VAL16:
		shortPtr := (*int16)(unsafe.Pointer(col.buffer))
		*val = *shortPtr
	case C.A_UVAL16:
		shortPtr := (*uint16)(unsafe.Pointer(col.buffer))
		*val = *shortPtr
	case C.A_VAL8:
		bytePtr := (*int8)(unsafe.Pointer(col.buffer))
		*val = *bytePtr
	case C.A_UVAL8:
		bytePtr := (*uint8)(unsafe.Pointer(col.buffer))
		*val = *bytePtr
	case C.A_FLOAT:
		floatPtr := (*float32)(unsafe.Pointer(col.buffer))
		*val = *floatPtr
	default:
		return errors.New("Unknown type")
	}

	return nil
}

func convertToLargeByteArray(buffer *C.char, size uint) []byte {
	res := make([]byte, size)
	s := res[:]
	ptr := uintptr(unsafe.Pointer(buffer))
	for size > MAX_BINARY_SIZE {
		copy(s, (*[MAX_BINARY_SIZE]byte)(unsafe.Pointer(ptr))[:])
		s = s[MAX_BINARY_SIZE:]
		size -= MAX_BINARY_SIZE
		ptr += MAX_BINARY_SIZE
	}
	if size > 0 {
		copy(s, (*[MAX_BINARY_SIZE]byte)(unsafe.Pointer(ptr))[:size])
	}
	return res
}

//Ignores the context param since dbcapi does not support setting timeout on non-prepared statement
func (connection *connection) Ping(ctx context.Context) error {
	query := "SELECT 'PING' FROM SYS.DUMMY"
	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))
	stmt := C.dbcapi_execute_direct(connection.conn, sql)

	if stmt == nil {
		return driver.ErrBadConn
	}

	return nil
}

func (statement *statement) Close() error {
	C.dbcapi_free_stmt(statement.stmt)
	return nil
}

func (statement *statement) NumInput() int {
	return int(C.dbcapi_num_params(statement.stmt))
}

func (statement *statement) Exec(args []driver.Value) (driver.Result, error) {
	for i := 0; i < len(args); i++ {
		err := statement.bindParameter(i, args[i])
		if err != nil {
			return nil, err
		}
	}

	execRes := C.dbcapi_execute(statement.stmt)
	if execRes != 1 {
		return nil, getError(statement.conn)
	}

	funcCode := C.dbcapi_get_function_code(statement.stmt)
	if funcCode == FunctionCode_DDL {
		return driver.ResultNoRows, nil
	}

	return &result{stmt: statement.stmt, conn: statement.conn}, nil
}

func (statement *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	value := make([]driver.Value, len(args))
	for i := 0; i < len(args); i++ {
		value[i] = args[i].Value
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return statement.Exec(value)
	} else {
		err := setTimeout(statement, deadline.Sub(time.Now()).Seconds())
		if err != nil {
			return nil, err
		}
		return statement.Exec(value)
	}
}

func (statement *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	value := make([]driver.Value, len(args))
	for i := 0; i < len(args); i++ {
		value[i] = args[i].Value
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return statement.Query(value)
	} else {
		err := setTimeout(statement, deadline.Sub(time.Now()).Seconds())
		if err != nil {
			return nil, err
		}
		return statement.Query(value)
	}
}

func setTimeout(statement *statement, timeout float64) error {
	//Add +1 since float64 to int32 will cause some loss in precision
	timeoutRes := C.dbcapi_set_query_timeout(statement.stmt, C.dbcapi_i32(timeout+1))
	if timeoutRes != 1 {
		return getError(statement.conn)
	}

	return nil
}

func (statement *statement) Query(args []driver.Value) (driver.Rows, error) {
	for i := 0; i < len(args); i++ {
		err := statement.bindParameter(i, args[i])
		if err != nil {
			return nil, err
		}
	}

	execRes := C.dbcapi_execute(statement.stmt)
	if execRes != 1 {
		return nil, getError(statement.conn)
	}

	return &rows{stmt: statement.stmt, conn: statement.conn}, nil
}

func (result *result) LastInsertId() (int64, error) {
	return -1, errors.New("Feature not supported")
}

func (result *result) RowsAffected() (int64, error) {
	rowsAffected := C.dbcapi_affected_rows(result.stmt)
	if rowsAffected == -1 {
		return 0, getError(result.conn)
	} else {
		return int64(rowsAffected), nil
	}
}

func (statement *statement) bindParameter(index int, paramVal driver.Value) error {
	bindD := (*C.struct_dbcapi_bind_data)(C.malloc(DBCAPI_BIND_DATA_STRUCT_LEN))
	defer C.free(unsafe.Pointer(bindD))
	descRes := C.dbcapi_describe_bind_param(statement.stmt, C.dbcapi_u32(index), bindD)
	if descRes != 1 {
		return getError(statement.conn)
	}

	var i C.dbcapi_bool = 0
	if paramVal == nil {
		i = C.dbcapi_bool(1)
		bindD.value.is_null = &i
	}

	if i != 1 {
		v := reflect.ValueOf(paramVal)
		switch v.Kind() {
		case reflect.Int64:
			len := C.size_t(8)
			bindD.value.length = &len
			iVal := v.Int()
			paramPtr := (*C.char)(unsafe.Pointer(&iVal))
			bindD.value.buffer = paramPtr
			bindD.value._type = C.A_VAL64
		case reflect.Bool:
			len := C.size_t(1)
			bindD.value.length = &len
			bVal := v.Bool()
			paramPtr := (*C.char)(unsafe.Pointer(&bVal))
			bindD.value.buffer = paramPtr
			bindD.value._type = C.A_VAL8
		case reflect.Float64:
			len := C.size_t(8)
			bindD.value.length = &len
			dVal := v.Float()
			paramPtr := (*C.char)(unsafe.Pointer(&dVal))
			bindD.value.buffer = paramPtr
			bindD.value._type = C.A_DOUBLE
		case reflect.String:
			strVal := v.String()
			a := make([]byte, len(strVal)+1)
			copy(a[:], strVal)
			bufSize := C.size_t(len(v.String()))
			bindD.value.length = &bufSize
			cStrVal := (*C.char)(unsafe.Pointer(&a[0]))
			bindD.value.buffer = cStrVal
			bindD.value._type = C.A_STRING
		case reflect.Slice:
			if bArr, ok := v.Interface().([]byte); ok {
				if len(bArr) > 0 {
					cStrVal := (*C.char)(unsafe.Pointer(&bArr[0]))
					bindD.value.buffer = cStrVal
				} else {
					a := make([]byte, 1)
					cStrVal := (*C.char)(unsafe.Pointer(&a[0]))
					bindD.value.buffer = cStrVal
				}
				bufSize := C.size_t(len(bArr))
				bindD.value.length = &bufSize
				bindD.value._type = C.A_BINARY
			} else {
				return errors.New("Unknown type")
			}
		default:
			if tVal, ok := v.Interface().(time.Time); ok {
				strVal := ""
				if tVal.Year() == 0 && tVal.Month() == 1 && tVal.Day() == 1 {
					strVal = tVal.Format(TIME_LAYOUT_STRING)
				} else if tVal.Hour() == 0 && tVal.Minute() == 0 && tVal.Second() == 0 && tVal.Nanosecond() == 0 {
					strVal = tVal.Format(DATE_LAYOUT_STRING)
				} else {
					strVal = tVal.Format(TS_LAYOUT_STRING)
				}
				a := make([]byte, len(strVal)+1)
				copy(a[:], strVal)
				bufSize := C.size_t(len(strVal))
				bindD.value.length = &bufSize
				cStrVal := (*C.char)(unsafe.Pointer(&a[0]))
				bindD.value.buffer = cStrVal
				bindD.value._type = C.A_STRING
			} else {
				return errors.New(fmt.Sprintf("Unknown type: %s ", v.Kind()))
			}
		}
	}

	bindRes := C.dbcapi_bind_param(statement.stmt, C.dbcapi_u32(index), bindD)
	if bindRes != 1 {
		return getError(statement.conn)
	}
	return nil
}

func (transaction *transaction) Commit() error {
	commitRes := C.dbcapi_commit(transaction.conn)
	if commitRes != 1 {
		return getError(transaction.conn)
	}

	autoCommRes := C.dbcapi_set_autocommit(transaction.conn, 1)
	if autoCommRes != 1 {
		return getError(transaction.conn)
	}

	err := setIsolationLevel(transaction.conn, DEFAULT_ISOLATION_LEVEL)
	if err != nil {
		return err
	}

	return nil
}

func (transaction *transaction) Rollback() error {
	rollbackRes := C.dbcapi_rollback(transaction.conn)
	if rollbackRes != 1 {
		return getError(transaction.conn)
	}

	autoCommRes := C.dbcapi_set_autocommit(transaction.conn, 1)
	if autoCommRes != 1 {
		return getError(transaction.conn)
	}

	err := setIsolationLevel(transaction.conn, DEFAULT_ISOLATION_LEVEL)
	if err != nil {
		return err
	}

	return nil
}

func (nullTime *NullTime) Scan(value interface{}) error {
	nullTime.Time, nullTime.Valid = value.(time.Time)
	return nil
}

func (nullTime NullTime) Value() (driver.Value, error) {
	if !nullTime.Valid {
		return nil, nil
	}
	return nullTime.Time, nil
}

func (nullBytes *NullBytes) Scan(value interface{}) error {
	nullBytes.Bytes, nullBytes.Valid = value.([]byte)
	return nil
}

func (nullBytes NullBytes) Value() (driver.Value, error) {
	if !nullBytes.Valid {
		return nil, nil
	}
	return nullBytes.Bytes, nil
}
