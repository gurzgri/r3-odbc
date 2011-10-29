/*******************************************************************************
**
**  Title:   ODBC Interface
**  File:  	 host-odbc.c
**
**  Purpose: Interface from REBOL3 to ODBC.
**
**  Version: 0.6.0
**  Date:    24-01-2011
**
**  Author:  Christian Ensel
**  Rights:  Copyright (C) Christian Ensel 2010-2011
**
**  This software is provided 'as-is', without any express or implied warranty.
**  In no event will the author be held liable for any damages arising from the
**	use of this software.
**
**  Permission is hereby granted, free of charge, to any person obtaining a copy
**  of this software and associated documentation files (the "Software"), to deal
**  in the Software without restriction, including without limitation the rights
**  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
**  copies of the Software, and to permit persons to whom the Software is
**  furnished to do so, subject to the following conditions:
**
**  The above copyright notice and this permission notice shall be included in
**  all copies or substantial portions of the Software.
**
**  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
**  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
**  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
**  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
**  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
**  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
**  THE SOFTWARE.
**
*******************************************************************************/

#define REB_EXT
#include <windows.h>
#include <reb-host.h>
#include <host-lib.h>
#include <sql.h>
#include <sqlext.h>

#define INCLUDE_EXT_DATA
#include <host-ext-odbc.h>

#define MAKE_ERROR(txt) ODBC_MakeError(frm, ODBC_SqlWCharToString(txt))
#define MAX_NUM_COLUMNS   255
#define COLUMN_TITLE_SIZE 255
#define hnull SQL_NULL_HANDLE                                                   // Abbreviation

enum GET_CATALOG   {GET_CATALOG_TABLES, GET_CATALOG_COLUMNS, GET_CATALOG_TYPES};// Used with ODBC_GetCatalog
enum FLATTEN_LEVEL {FLATTEN_NOT, FLATTEN_ONCE, FLATTEN_DEEP};                   // Used with ODBC_Flatten

typedef struct {                                                                // For binding parameters
	RXIARG       value;
	int          rebol_type;
	SQLLEN       size;
	void        *buffer;
	SQLLEN       length;
} PARAMETER;

typedef struct {                 												// For describing columns
	SQLWCHAR     title[COLUMN_TITLE_SIZE];\
	SQLSMALLINT  title_length;
	SQLSMALLINT  sql_type;
	SQLSMALLINT  c_type;
	int          rebol_type;
	SQLULEN      column_size;
	SQLPOINTER   buffer;
	SQLULEN      buffer_size;
	SQLULEN      buffer_length;
	SQLSMALLINT  precision;
	SQLSMALLINT  nullable;
	RXIARG       value;
} COLUMN;


/******************************************************************************/
void       Init_ODBC              (void);
RXIEXT int RXD_ODBC               (int cmd, RXIFRM *frm, void *data);

void       ODBC_Flatten           (RXIARG *nest, RXIARG *flat, enum FLATTEN_LEVEL level);

	   int ODBC_StringToSqlWChar  (REBSER   *source, SQLWCHAR *target);
	   int ODBC_UnCamelCase       (SQLWCHAR *source, SQLWCHAR *target);
REBSER*    ODBC_SqlWCharToString  (SQLWCHAR *source);
REBSER*    ODBC_SqlBinaryToBinary (char     *source, int length);

RXIEXT int ODBC_ConvertSqlToRebol (COLUMN *column);

RXIEXT int ODBC_MakeError         (RXIFRM *frm, REBSER *description);
RXIEXT int ODBC_ReturnError       (RXIFRM *frm, SQLSMALLINT handleType, SQLHANDLE handle);
void       ODBC_Close             (RXIFRM *frm); // conn, stmt
RXIEXT int ODBC_OpenDb            (RXIFRM *frm);
RXIEXT int ODBC_OpenSql           (RXIFRM *frm);
RXIEXT int ODBC_Insert            (RXIFRM *frm);
RXIEXT int ODBC_Copy              (RXIFRM *frm);

SQLRETURN  ODBC_BindParameter     (RXIFRM *frm, SQLHSTMT hstmt, PARAMETER *params, int p, int type);
SQLRETURN  ODBC_GetCatalog        (RXIFRM *frm, SQLHSTMT hstmt, enum GET_CATALOG which, REBSER *block);
SQLRETURN  ODBC_DescribeResults   (RXIFRM *frm, SQLHSTMT hstmt, int num_columns, COLUMN *columns, REBSER *titles);
SQLRETURN  ODBC_BindColumns       (RXIFRM *frm, SQLHSTMT hstmt, int num_columns, COLUMN *columns, RXIARG *values);
/******************************************************************************/



#ifndef ODBC_DLL
/*******************************************************************************
**
*/	void Init_ODBC(void)
/*
**  ODBC host-kit extension initialisation
**
*******************************************************************************/
{
	RL = (RL_LIB *)RL_Extend((REBYTE *)(&RX_odbc[0]), (RXICAL)&RXD_ODBC);
}
#else
RL_LIB *RL;
/*******************************************************************************
**
*/  const char *RX_Init(int opts, RL_LIB *lib)
/*
**  ODBC external DLL extension initialisation
**
*******************************************************************************/
{
	RL = lib;
	return RX_odbc;
}
#endif


/*******************************************************************************
**
*/
#ifndef ODBC_DLL
		RXIEXT int RXD_ODBC(int cmd, RXIFRM *frm, void *data)
#else
		int RX_Call(int cmd, RXIFRM *frm, void *data)
#endif
/*
**		ODBC command extension dispatcher.
**
*******************************************************************************/
{
	REBSER   *database, *statement;
	RXIARG    value, flat, nest;
	SQLHSTMT  hstmt;

	switch (cmd)
	{
		case CMD_ODBC_OPEN_CONNECTION:
			return ODBC_OpenDb(frm);

		case CMD_ODBC_OPEN_STATEMENT:
			return ODBC_OpenSql(frm);

		case CMD_ODBC_INSERT_ODBC:
			return ODBC_Insert(frm);

		case CMD_ODBC_UPDATE_ODBC:
			return ODBC_Update(frm);

		case CMD_ODBC_COPY_ODBC:
			return ODBC_Copy(frm);

		case CMD_ODBC_CLOSE_ODBC:
			ODBC_Close(frm);
			return RXR_NO_COMMAND;

		case CMD_ODBC_FLATTEN:
			nest        = RXA_ARG(frm, 1);
			flat.series = RL_MAKE_BLOCK(RL_SERIES(nest.series, RXI_SER_TAIL));
			flat.index  = 0;

			ODBC_Flatten(&nest, &flat, RXA_REF(frm, 2) ? FLATTEN_DEEP : FLATTEN_ONCE);

			RXA_SERIES(frm, 1) = flat.series;
			RXA_INDEX (frm, 1) = 0;
			RXA_TYPE  (frm, 1) = RXT_BLOCK;

			return RXR_VALUE;

		default:
			return RXR_NO_COMMAND;
	}
}


/*******************************************************************************
**
*/	void ODBC_Flatten(RXIARG *nest, RXIARG *flat, enum FLATTEN_LEVEL level)
/*
**  	Flattens a block of blocks. Depending on LEVEL, nested blocks are
**		flattened or not.
**
**  	Arguments:
**          nest  - The nested source block
**			flat  - The flattened result block
**			level - One of FLATTEN_LEVEL
**
*******************************************************************************/
{
	RXIARG item;
	u32    type, i;

	for (i = nest->index; i < RL_SERIES(nest->series, RXI_SER_TAIL); i++)
	{
		type = RL_GET_VALUE(nest->series, i, &item);

		if (type == RXT_BLOCK && level) {
			ODBC_Flatten(&item, flat, level == FLATTEN_ONCE ? FLATTEN_NOT : FLATTEN_DEEP);
		}
		else {
			RL_SET_VALUE(flat->series, RL_SERIES(flat->series, RXI_SER_TAIL), item, type);
		}
	}
}


/*------------------------------------------------------------------------------
**
*/  int ODBC_StringToSqlWChar(REBSER *source, SQLWCHAR *target)
/*
/*----------------------------------------------------------------------------*/
{
	int i;

	for (i = 0; i < RL_SERIES(source, RXI_SER_TAIL); i++) target[i] = RL_GET_CHAR(source, i);

	return i;
}


/*------------------------------------------------------------------------------
**
*/	REBSER* ODBC_SqlWCharToString(SQLWCHAR *source)
/*
/*----------------------------------------------------------------------------*/
{
	int     i, length = lstrlenW(source);
	REBSER *target = RL_MAKE_STRING(length, TRUE); // UTF-8 for REBOL3

	for (i = 0; i < length; i++) RL_SET_CHAR(target, i, source[i]);

	return target;
}


/*------------------------------------------------------------------------------
**
*/	REBSER* ODBC_SqlBinaryToBinary(char *source, int length)
/*
/*----------------------------------------------------------------------------*/
{
	int     i;
	REBSER *target = RL_MAKE_STRING(length, FALSE);

	for (i = 0; i < length; i++) RL_SET_CHAR(target, i, source[i]);

	return target;
}


/*******************************************************************************
**
*/	int ODBC_UnCamelCase(SQLWCHAR *source, SQLWCHAR *target)
/*
*******************************************************************************/
{
	int length = lstrlenW(source), s, t = 0;
	WCHAR *hyphen = L"-", *underscore = L"_";

	for (s = 0; s < length; s++)
	{
		target[t++] = (source[s] == *underscore) ? *hyphen : towlower(source[s]);

		if (
			(s < length - 2 && iswupper(source[s]) && iswupper(source[s + 1]) && iswlower(source[s + 2])) ||
			(s < length - 1 && iswlower(source[s]) && iswupper(source[s + 1]))
		){
			target[t++] = *hyphen;
		}
	}
	target[t++] = 0;

	return t;
}


/*******************************************************************************
**
*/	RXIEXT int ODBC_MakeError(RXIFRM *frm, REBSER *description)
/*
*******************************************************************************/
{
	REBSER *block, *args;
	RXIARG  value;

	block = RL_MAKE_BLOCK(3);
	args  = RL_MAKE_BLOCK(1);

	value.int32a = RL_MAP_WORD("odbc");	 RL_SET_VALUE(block, 0, value, RXT_LIT_WORD);
	value.int32a = RL_MAP_WORD("error"); RL_SET_VALUE(block, 1, value, RXT_LIT_WORD);
	value.series = description;			 RL_SET_VALUE(args,  0, value, RXT_STRING);
	value.series = args;				 RL_SET_VALUE(block, 2, value, RXT_BLOCK);

	RXA_SERIES(frm, 1) = block;
	RXA_INDEX (frm, 1) = 0;
	RXA_TYPE  (frm, 1) = RXT_BLOCK;

	return RXR_VALUE;
}


/*******************************************************************************
**
*/	RXIEXT int ODBC_ReturnError(RXIFRM *frm, SQLSMALLINT handleType, SQLHANDLE handle)
/*
*******************************************************************************/
{
	RXIARG       value;
	REBSER      *block;
	SQLWCHAR	 state[6], message[4086];
	SQLINTEGER	 native;
	SQLSMALLINT  buffer = 4086, message_len = 0;
	SQLRETURN	 rc;

	value.series = (REBSER *)ODBC_SqlWCharToString(L"unknown error");
	value.index  = 0;

	rc = SQLGetDiagRecW(handleType, handle, 1, state, &native, message, buffer, &message_len);
	if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) value.series = (REBSER *)ODBC_SqlWCharToString(message);

	return ODBC_MakeError(frm, value.series);
}


/*******************************************************************************
**
*/	void ODBC_Close(RXIFRM *frm) // db/none, stmt/none
/*
*******************************************************************************/
{
	REBSER      *statement, *connection;
	RXIARG      *values, value;
	SQLHENV 	 henv;
	SQLHDBC 	 hdbc;
	SQLHSTMT	 hstmt;
	COLUMN      *columns;
	int          type;

	if (RXA_TYPE(frm, 2) == RXT_OBJECT)
	{
		statement = RXA_OBJECT(frm, 2);

		hstmt   = (RL_GET_FIELD(statement, RL_MAP_WORD("statement"), &value) == RXT_HANDLE) ? value.addr : NULL;
		columns = (RL_GET_FIELD(statement, RL_MAP_WORD("columns"),   &value) == RXT_HANDLE) ? value.addr : NULL;
		values  = (RL_GET_FIELD(statement, RL_MAP_WORD("values"),    &value) == RXT_HANDLE) ? value.addr : NULL;

		if (hstmt)   SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
		if (columns) free(columns);
		if (values)  free(values);

		return;
	}

	if (RXA_TYPE(frm, 1) == RXT_OBJECT)
	{
		connection = RXA_OBJECT(frm, 1);

		henv = (RL_GET_FIELD(connection, RL_MAP_WORD("environment"), &value) == RXT_HANDLE) ? value.addr : NULL;
		hdbc = (RL_GET_FIELD(connection, RL_MAP_WORD("connection"),  &value) == RXT_HANDLE) ? value.addr : NULL;

		if (hdbc) SQLDisconnect(hdbc);
		if (hdbc) SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
		if (henv) SQLFreeHandle(SQL_HANDLE_ENV, henv);

		return;
	}
}


/*******************************************************************************
**
*/	RXIEXT int ODBC_OpenDb(RXIFRM *frm)
/*
**		Opens a ODBC connection, returns handles to henv and hdbc as an
**		object which is to be passed to consecuting functions.
**
*******************************************************************************/
{
	SQLHENV 	 henv;
	SQLHDBC 	 hdbc;
	SQLWCHAR    *connect;
	SQLRETURN	 rc;
	SQLSMALLINT  out;
	i32      	 length, in;
	REBSER      *string, *database;
	RXIARG	     value;
	int          type, error;

	database = RXA_OBJECT(frm, 1);
	string   = RXA_SERIES(frm, 2);
	length   = RL_SERIES(string, RXI_SER_TAIL);

	connect = malloc(sizeof(SQLWCHAR) * length);								// Allocate the connection string
	if (connect == NULL) return MAKE_ERROR(L"Couldn't allocate connection string!");

	ODBC_StringToSqlWChar(string, connect);

	rc = SQLAllocHandle(SQL_HANDLE_ENV, hnull, &henv);                          // Allocate the environment handle
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_ENV, SQL_NULL_HENV);

	value.addr = henv;
	RL_SET_FIELD(database, RL_MAP_WORD("environment"), value, RXT_HANDLE);

	rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);	// Set the ODBC3 version environment attribute
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{
		error = ODBC_ReturnError(frm, SQL_HANDLE_ENV, henv);
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		return error;
	}

	rc = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);							// Allocate the connection handle
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{
		error = ODBC_ReturnError(frm, SQL_HANDLE_ENV, henv);
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		return error;
	}

	value.addr = hdbc;
	RL_SET_FIELD(database, RL_MAP_WORD("connection"), value, RXT_HANDLE);

	rc = SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);			// Set login timeout to 5 seconds (why 5?)
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{
		error = ODBC_ReturnError(frm, SQL_HANDLE_ENV, henv);
		SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		return error;
	}

	rc = SQLDriverConnectW(hdbc, NULL, 											// Connect to the Driver
			(SQLWCHAR*)connect, length, NULL, 0, &out, SQL_DRIVER_NOPROMPT
	);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{
		error = ODBC_ReturnError(frm, SQL_HANDLE_ENV, henv);
		SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		return error;
	}

	free(connect);

	return RXR_TRUE;
}


/*******************************************************************************
**
*/	RXIEXT int ODBC_OpenSql(RXIFRM *frm)
/*
*******************************************************************************/
{
	SQLHDBC      hdbc;
	SQLHSTMT     hstmt;
	SQLRETURN    rc;
	REBSER      *database, *statement;
	RXIARG       value;
	int          type;

	database  = RXA_OBJECT(frm, 1);
	statement = RXA_OBJECT(frm, 2);

	type = RL_GET_FIELD(database, RL_MAP_WORD("connection"), &value);			// Get connection handle
	if (type != RXT_HANDLE) return MAKE_ERROR(L"Invalid connection argument!");

	hdbc = value.addr;

	rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);							// Allocate the statement handle
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_DBC, hdbc);

	value.addr = hstmt;
	RL_SET_FIELD(statement, RL_MAP_WORD("statement"), value, RXT_HANDLE);

	return RXR_TRUE;
}

//a: b: c: 0 dt [loop 512 [cache:      open odbc://cachesamples a: a + 1                               close cache      b: b + 1]]
//a: b: c: 0 dt [loop 512 [cache:      open odbc://cachesamples a: a + 1 db: first cache      b: b + 1 close cache      c: c + 1]]
//dbs: [] a: 0 dt [cache: open odbc://cachesamples loop 512 [append dbs first cache a: a + 1] close cache]

//a: b: c: 0 dt [loop 512 [postgresql: open odbc://pgsamples    a: a + 1                               close postgresql b: b + 1]]
//a: b: c: 0 dt [loop 512 [postgresql: open odbc://pgsamples    a: a + 1 db: first postgresql b: b + 1 close postgresql c: c + 1]]


/*******************************************************************************
**
*/	RXIEXT int ODBC_Update(RXIFRM *frm)
/*
*******************************************************************************/
{
	SQLHDBC      hdbc;
	SQLRETURN    rc;
	RXIARG       value;
	REBSER      *database;
	i64          access, commit;
	int          type;

	database  = RXA_OBJECT(frm, 1);
	access    = RXA_LOGIC (frm, 2);
	commit    = RXA_LOGIC (frm, 3);

	type = RL_GET_FIELD(database, RL_MAP_WORD("connection"), &value);			// Get connection handle
	if (type != RXT_HANDLE) return MAKE_ERROR(L"Invalid connection argument!");

	hdbc = value.addr;

	rc = SQLSetConnectAttr(hdbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER *)(access ? SQL_MODE_READ_WRITE : SQL_MODE_READ_ONLY), SQL_IS_UINTEGER);							// Allocate the statement handle
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_DBC, hdbc);

	rc = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,  (SQLPOINTER *)(commit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF), SQL_IS_UINTEGER);							// Allocate the statement handle
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_DBC, hdbc);

	return RXR_TRUE;
}


/*******************************************************************************
**
*/	SQLRETURN ODBC_BindParameter(RXIFRM *frm, SQLHSTMT hstmt, PARAMETER *params, int p, int rebol_type)
/*
**	Arguments:
**		params - buffer where to store bound parameter values (to not conflict
**               wiith being gc'ed on the REBOL side)
**
**  The buffer at *ParameterValuePtr SQLBindParameter binds to is deferred
**  buffer, and so is the StrLen_or_IndPtr. They need to be vaild over until
**  Execute or ExecDirect are called.
**
*******************************************************************************/
{
	int          tail, hour, minute, second, i;
	REBSER      *series;
	TIME_STRUCT *time;
	DATE_STRUCT	*date;
	WCHAR		*chars;
	char        *bytes;
	SQLSMALLINT  c_type, sql_type;
	SQLPOINTER   param;
	SQLLEN       buffer_size;
	SQLLEN       length = 0, column_size;
	SQLRETURN    rc;

	buffer_size 	 	 = 0;
	params[p].length 	 = 0;
	params[p].size   	 = 0;
	params[p].rebol_type = rebol_type;

	switch (rebol_type)
	{
		case RXT_TIME:
			time = malloc(sizeof(TIME_STRUCT));

			time->hour   = (params[p].value.int64 / 3.6e12);
			time->minute = (params[p].value.int64 - (time->hour * 3.6e12)) / 6e10;
			time->second = (params[p].value.int64 - (time->hour * 3.6e12) - (time->minute * 60e9)) / 1000e6;

			column_size = sizeof(TIME_STRUCT);
			params[p].buffer = time;
			params[p].size   = column_size;
			params[p].length = column_size;
			break;

		case RXT_DATE:
			date = malloc(sizeof(DATE_STRUCT));

			date->year   = (params[p].value.int32a & 1073676288) >> 16;
			date->month  = (params[p].value.int32a & 	  61440) >> 12;
			date->day    = (params[p].value.int32a &       3968) >>  7;

			column_size = sizeof(DATE_STRUCT);
			params[p].buffer = date;
			params[p].size   = column_size;
			params[p].length = column_size;
			break;

		case RXT_STRING:
			series = params[p].value.series;
			tail   = RL_SERIES(series, RXI_SER_TAIL);

			buffer_size = sizeof(SQLWCHAR) * tail;
			chars  		= malloc(buffer_size);
			if (chars == NULL) return MAKE_ERROR(L"Couldn't allocate parameter buffer!");

			length 		= ODBC_StringToSqlWChar(series, chars);
			column_size = 2 * length;

			params[p].buffer = chars;
			params[p].size   = column_size;
			params[p].length = column_size;
			break;

		case RXT_BINARY:
			series = params[p].value.series;
			tail   = RL_SERIES(series, RXI_SER_TAIL);

			buffer_size = sizeof(char) * tail;
			bytes       = malloc(buffer_size);

			for (i = 0; i < tail; i++) bytes[i] = RL_GET_CHAR(series, i);

			params[p].buffer = bytes;
			params[p].size   = tail;
			params[p].length = tail;
			break;
	}

	switch (rebol_type)
	{
		case RXT_INTEGER: 	c_type = SQL_C_LONG; 		sql_type = SQL_INTEGER; 	param = &(params[p].value.int64);	break;
		case RXT_DECIMAL: 	c_type = SQL_C_DOUBLE; 		sql_type = SQL_DOUBLE; 		param = &(params[p].value.dec64);	break;
		case RXT_LOGIC: 	c_type = SQL_C_BIT; 		sql_type = SQL_BIT; 		param = &(params[p].value.int64);	break;
		case RXT_DATE: 		c_type = SQL_C_TYPE_DATE; 	sql_type = SQL_TYPE_DATE; 	param = params[p].buffer; 	        break;
		case RXT_TIME: 		c_type = SQL_C_TYPE_TIME; 	sql_type = SQL_TYPE_TIME; 	param = params[p].buffer;			break;
		case RXT_STRING:	c_type = SQL_C_WCHAR;		sql_type = SQL_VARCHAR;     param = chars;						break;
		case RXT_BINARY:	c_type = SQL_C_BINARY;		sql_type = SQL_VARBINARY;   param = bytes;						break;
		case RXT_NONE:
		default:		 	c_type = SQL_C_DEFAULT;		sql_type = SQL_NULL_DATA;	params[p].length = SQL_NULL_DATA;   break;
	}

	// API call to SQLBindParameter
	rc = SQLBindParameter(hstmt, p, SQL_PARAM_INPUT, c_type, sql_type, params[p].size, 0, param, buffer_size, &params[p].length);
	return rc;
}


/*##############################################################################
##
*/  SQLRETURN ODBC_GetCatalog(RXIFRM *frm, SQLHSTMT hstmt, enum GET_CATALOG which, REBSER *block)
/*
##############################################################################*/
{
	SQLWCHAR     pattern[4][255];
	SQLSMALLINT  length[4];
	int          arg;
	RXIARG       value;

	for (arg = 0; arg < 4; arg++)
	{
		if (RL_GET_VALUE(block, arg + 1, &value) == RXT_STRING)
		{
			length[arg] = RL_SERIES(value.series, RXI_SER_TAIL);
			ODBC_StringToSqlWChar(value.series, &pattern[arg][0]);
		}
		else length[arg] = 0;
	}

	switch (which)
	{
		case GET_CATALOG_TABLES:
			return SQLTablesW(hstmt,
				length[2] == 0 ? NULL : (SQLWCHAR *)&(pattern[2][0]), length[2], // catalog
				length[1] == 0 ? NULL : (SQLWCHAR *)&(pattern[1][0]), length[1], // schema
				length[0] == 0 ? NULL : (SQLWCHAR *)&(pattern[0][0]), length[0], // table
				length[3] == 0 ? NULL : (SQLWCHAR *)&(pattern[3][0]), length[3]  // type
			);
			break;

		case GET_CATALOG_COLUMNS:
			return SQLColumnsW(hstmt,
				length[3] == 0 ? NULL : (SQLWCHAR *)&(pattern[3][0]), length[3], // catalog
				length[2] == 0 ? NULL : (SQLWCHAR *)&(pattern[2][0]), length[2], // schema
				length[0] == 0 ? NULL : (SQLWCHAR *)&(pattern[0][0]), length[0], // table
				length[1] == 0 ? NULL : (SQLWCHAR *)&(pattern[1][0]), length[1]  // column
			);
			break;

		case GET_CATALOG_TYPES:
			return SQLGetTypeInfoW(hstmt, SQL_ALL_TYPES);
			break;
	}
}


/*******************************************************************************
**
*/  SQLRETURN ODBC_DescribeResults(RXIFRM *frm, SQLHSTMT hstmt, int num_columns, COLUMN *columns, REBSER *titles)
/*
**  Sets up the COLUMNS description, retrievs column titles and column descriptions
**
*******************************************************************************/
{
	SQLSMALLINT  col;
	COLUMN      *column;
	SQLRETURN    rc;
	SQLWCHAR    *title;
	int          length;
	char         rebol_word[COLUMN_TITLE_SIZE];
	RXIARG       value;

	for (col = 0; col <= num_columns - 1; col++)
	{
		column = &columns[col];

		rc = SQLDescribeColW(hstmt, (SQLSMALLINT)(col + 1),
			&column->title[0], COLUMN_TITLE_SIZE,
			&column->title_length,
			&column->sql_type,
			&column->column_size,
			&column->precision,
			&column->nullable
		);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

		title = malloc(sizeof(WCHAR) * (column->title_length * 2 + 2));
		if (title == NULL) return MAKE_ERROR(L"Couldn't allocate column name buffer!");

		length  = ODBC_UnCamelCase(column->title, title);
		WideCharToMultiByte(CP_UTF8, 0, (LPCWSTR)title, length, (LPSTR)&rebol_word, COLUMN_TITLE_SIZE, NULL, NULL);

		value.int32a = RL_MAP_WORD(rebol_word);
		RL_SET_VALUE(titles, col, value, RXT_WORD);

		free(title);
	}

	return SQL_SUCCESS;
}


/*******************************************************************************
**
*/  SQLRETURN ODBC_BindColumns(RXIFRM *frm, SQLHSTMT hstmt, int num_columns, COLUMN *columns, RXIARG *values)
/*
*******************************************************************************/
{
	RXIARG       value;
	SQLSMALLINT  col;
	COLUMN      *column;
	SQLSMALLINT  c_type;
	void        *buffer;
	int          buffer_size;
	SQLRETURN    rc;

	for (col = 0; col <= num_columns - 1; col++)
	{
		column = &columns[col];
		column->value.int64 = 0;

		//-- Bind columns
		//
		switch (column->sql_type)
		{
			case SQL_SMALLINT: case SQL_INTEGER: case SQL_TINYINT: case SQL_BIGINT:
				c_type 		= SQL_C_LONG;
				buffer      = &column->value.int64;
				buffer_size = sizeof(i64);
				break;

			case SQL_DECIMAL: case SQL_NUMERIC: case SQL_REAL: case SQL_FLOAT: case SQL_DOUBLE:
				c_type 	    = SQL_C_DOUBLE;
				buffer      = &column->value.dec64;
				buffer_size = sizeof(double);
				break;

			case SQL_TYPE_DATE:
				c_type      = SQL_C_TYPE_DATE;
				buffer_size = sizeof(DATE_STRUCT);
				buffer      = malloc(buffer_size);
				if (buffer == NULL) return MAKE_ERROR(L"Couldn't allocate date struct buffer!");
				break;

			case SQL_TYPE_TIME:
				c_type      = SQL_C_TYPE_TIME;
				buffer_size = sizeof(TIME_STRUCT);
				buffer      = malloc(buffer_size);
				if (buffer == NULL) return MAKE_ERROR(L"Couldn't allocate time struct buffer!");
				break;

			case SQL_BIT:
				c_type      = SQL_C_BIT;
				buffer      = (char *)&column->value.int64;
				buffer_size = sizeof(i64);
				break;

			case SQL_BINARY: case SQL_VARBINARY: case SQL_LONGVARBINARY:
				c_type      = SQL_C_BINARY;
				buffer_size = sizeof(char) * column->column_size;
				buffer      = malloc(buffer_size);
				if (buffer == NULL) return MAKE_ERROR(L"Couldn't allocate binary buffer!");
				break;

			case SQL_CHAR: case SQL_VARCHAR: case SQL_LONGVARCHAR: case SQL_WCHAR: case SQL_WVARCHAR: case SQL_WLONGVARCHAR:
			default:
				c_type      = SQL_C_WCHAR;
				buffer_size = sizeof(WCHAR) * (column->column_size + 1);
				buffer      = malloc(buffer_size);
				if (buffer == NULL) return MAKE_ERROR(L"Couldn't allocate string buffer!");
				break;
		}

		column->c_type      = c_type;
		column->buffer      = buffer;
		column->buffer_size = buffer_size;

		rc = SQLBindCol(hstmt, (SQLSMALLINT)(col + 1),
						  column->c_type,
			 (SQLPOINTER*)column->buffer,
						  column->buffer_size,
			(SQLINTEGER*)&column->buffer_length
		);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);
	}

	return SQL_SUCCESS;
}


/*******************************************************************************
**
*/	RXIEXT int ODBC_ConvertSqlToRebol(COLUMN *column)
/*
*******************************************************************************/
{
	TIME_STRUCT *time;
	DATE_STRUCT *date;

	if (column->buffer_length == SQL_NULL_DATA) return RXT_NONE;

	switch (column->sql_type)
	{
		case SQL_SMALLINT: case SQL_INTEGER:
			return RXT_INTEGER;

		case SQL_NUMERIC: case SQL_REAL: case SQL_FLOAT: case SQL_DOUBLE:
			return RXT_DECIMAL;

		case SQL_TYPE_DATE:
			date = (DATE_STRUCT *)column->buffer;
			column->value.int32a = (date->year << 16) | (date->month << 12) | (date->day << 7);
			return RXT_DATE;

		case SQL_TYPE_TIME:
			time = (TIME_STRUCT *)column->buffer;
			column->value.int64 = (time->hour * 3.6e12) + (time->minute * 6e10) + (time->second * 1e9);
			return RXT_TIME;

		case SQL_BIT:
			return RXT_LOGIC;

		case SQL_BINARY: case SQL_VARBINARY: case SQL_LONGVARBINARY:
			column->value.series = (REBSER *)ODBC_SqlBinaryToBinary((char *)column->buffer, column->buffer_length);
			column->value.index  = 0;
			return RXT_BINARY;

		default:
			column->value.series = (REBSER *)ODBC_SqlWCharToString((SQLWCHAR *)column->buffer);
			column->value.index  = 0;
			return RXT_STRING;
	}
}


/*##############################################################################
##
*/	RXIEXT int ODBC_Insert(RXIFRM *frm)
/*
##  Executes an SQL statement.
##
##  Returns:
##      TRUE for row changing statements, BLOCK! of column titles for
##      'select'-statements.
##      Prepares and executes statements on first pass, only executes them
##      on consecutive envocation.
##
##############################################################################*/
{
	REBSER      *object, *arguments, *statement, *previous;
	RXIARG       value;
	RXIARG      *values;
	i32          index = 0, position, tail,
				 length, p, num_params;
	WCHAR       *string;
	SQLRETURN    rc;
	SQLULEN      row, num_rows, max_rows;
	SQLSMALLINT  col, num_columns;
	SQLHSTMT     hstmt;
	RXIARG       v;
	int          type, rebol_type, pos = 0, prepare, execute, direct;
	PARAMETER   *params;
	COLUMN      *columns;
	REBSER      *titles, *records;

	object     = RXA_OBJECT(frm, 1);											// Retrieve the statement object / statement handle
	hstmt      = (RL_GET_FIELD(object, RL_MAP_WORD("statement"), &value) == RXT_HANDLE) ? (SQLHSTMT)value.addr : hnull;
	if (hstmt == NULL) return MAKE_ERROR(L"Invalid statement object!");
	row        = 0;
	direct     = TRUE; prepare = FALSE; execute = FALSE;

	SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
	SQLCloseCursor(hstmt);

	//-- Set number of rows returned by driver --
	//
	// This is in the wrong place here
	// max_rows = 0;
	// rc = SQLSetStmtAttr(hstmt, SQL_ATTR_MAX_ROWS, &max_rows, SQL_IS_POINTER);
	// if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

	arguments  = RXA_SERIES(frm, 2);

	switch (RL_GET_VALUE(arguments, 0, &value))
	{
		// Execute catalog function, when first element in the argument block is a (catalog) word
		//
		case RXT_WORD:
		{
			if      (value.int32a == RL_MAP_WORD("tables"))
				rc = ODBC_GetCatalog(frm, hstmt, GET_CATALOG_TABLES,  arguments);
			else if (value.int32a == RL_MAP_WORD("columns"))
				rc = ODBC_GetCatalog(frm, hstmt, GET_CATALOG_COLUMNS, arguments);
			else if (value.int32a == RL_MAP_WORD("types"))
				rc = ODBC_GetCatalog(frm, hstmt, GET_CATALOG_TYPES,   arguments);
			break;
		}

		// Prepare/Execute statement, when first element in the block is a (statement) string
		//
		case RXT_STRING:
		{
			// retrieve supplied statement
			statement = value.series;
			tail      = RL_SERIES(statement, RXI_SER_TAIL);

			// compare with previously prepared statement
			previous  = (REBSER*)(RL_GET_FIELD(object, RL_MAP_WORD("string"), &value) == RXT_HANDLE) ? value.addr : hnull;

			// prepare statement
			if (statement != previous)
			{
				string    = malloc(sizeof(WCHAR) * tail);
				if (string == NULL) return MAKE_ERROR(L"Couldn't allocate statement buffer!");

				length 	  = ODBC_StringToSqlWChar(statement, string);

				rc = SQLPrepare(hstmt, (SQLWCHAR *)string, length);
				if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

				value.addr = statement; RL_SET_FIELD(object, RL_MAP_WORD("string"), value, RXT_HANDLE); // remember statement string handle
			}

			// bind parameters
			//
			if (0 < (num_params = RL_SERIES(arguments, RXI_SER_TAIL) - 1)) // the statement string doesn't count as an argument
			{
				// Allocate parameter buffer.
				//
				params = malloc(sizeof(PARAMETER) * (num_params + 1));
				if (params == NULL) return MAKE_ERROR(L"Couldn't allocate parameter buffer!");

				// Bind parameters
				//
				for (p = 1; p <= num_params; p++)
				{
					type = RL_GET_VALUE(arguments, ++index, &params[p].value);

					rc   = ODBC_BindParameter(frm, hstmt, params, p, type);
					if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);
				}
			}

			// execute statement
			//
			rc = SQLExecute(hstmt);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

			// free param buffers
			//
			if (0 < num_params)
			{
				type = RL_GET_FIELD(object, RL_MAP_WORD("parameters"), &value); // unproteced
				if (type == RXT_HANDLE && (params = value.addr))
				{
					for (p = 1; p <= num_params; p++) switch (params[p].rebol_type) case RXT_STRING: RXT_BINARY: RXT_DATE: RXT_TIME: free(params[p].buffer);
					free(params);
				}
				RL_SET_FIELD(object, RL_MAP_WORD("parameters"), value, RXT_NONE);
				//RL_PRINT("ok\n", 0);
			}

			// free statement
			//
			if (statement != previous) free(string);

			break;
		}

		// unknown dialect, when first element in the block is something other
		//
		default:
			return MAKE_ERROR(L"Cannot parse dialect!");
	}

	// Return early with number of rows affected for Insert/Update/Delete statements
	//
	rc = SQLNumResultCols(hstmt, &num_columns);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

	if (!num_columns)
	{
		// statement changes rows (insert/update/delete)
		//
		rc = SQLRowCount(hstmt, &num_rows);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

		RXA_INT64(frm, 1) = num_rows;
		RXA_TYPE (frm, 1) = RXT_INTEGER;
	}
	else
	{
		// statement returns result-set (select, catalog)
		//
		if (statement != previous)
		{
			type = RL_GET_FIELD(object, RL_MAP_WORD("columns"), &value); // unproteced
			if (type == RXT_HANDLE) free(value.addr);
			type = RL_GET_FIELD(object, RL_MAP_WORD("values"),  &value); // unproteced
			if (type == RXT_HANDLE) free(value.addr);

			columns = malloc(sizeof(COLUMN) * num_columns);
			values  = malloc(sizeof(RXIARG) * num_columns);
			titles  = RL_MAKE_BLOCK(num_columns); //GC'ed by REBOL

			if (!columns || !titles || !values) return MAKE_ERROR(L"Couldn't allocate column buffers!");

			value.addr = columns;	RL_SET_FIELD(object, RL_MAP_WORD("columns"), value, RXT_HANDLE);
			value.addr = values;	RL_SET_FIELD(object, RL_MAP_WORD("values"),  value, RXT_HANDLE);

			rc = ODBC_DescribeResults(frm, hstmt, num_columns, columns, titles);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

			value.addr = titles; RL_SET_FIELD(object, RL_MAP_WORD("titles"), value, RXT_HANDLE); // remember column titles

			rc = ODBC_BindColumns(frm, hstmt, num_columns, columns, values);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);
		}
		else
		{
			titles = (REBSER*)(RL_GET_FIELD(object, RL_MAP_WORD("titles"), &value) == RXT_HANDLE) ? value.addr : hnull; // retrieve column titles from previous preparation
			if (!titles) return MAKE_ERROR(L"Couldn't retrieve previous column titles!");
		}

		// Store column titles
		//
		RXA_SERIES(frm, 1) = titles;
		RXA_INDEX (frm, 1) = 0;
		RXA_TYPE  (frm, 1) = RXT_BLOCK;
	}

	return RXR_VALUE;
}


/*******************************************************************************
**
*/	RXIEXT int ODBC_Copy(RXIFRM *frm)
/*
**  Returns the result set for SQL-Select statements and catalog functions as
**  as a (result-set) block of (row) blocks.
**
*******************************************************************************/
{
	COLUMN      *columns, *column;
	RXIARG      *values, value;
	REBSER      *object, *records, *record;
	SQLHSTMT     hstmt;
	SQLSMALLINT  col, num_columns;
	SQLULEN      row;
	SQLRETURN    rc;
	int          rebol_type;

	object  = RXA_OBJECT(frm, 1); // statement object

	hstmt   = (SQLHSTMT*)(RL_GET_FIELD(object, RL_MAP_WORD("statement"), &value) == RXT_HANDLE) ? value.addr : NULL;
	columns = (COLUMN  *)(RL_GET_FIELD(object, RL_MAP_WORD("columns"),   &value) == RXT_HANDLE) ? value.addr : NULL;
	values  = (RXIARG  *)(RL_GET_FIELD(object, RL_MAP_WORD("values"),    &value) == RXT_HANDLE) ? value.addr : NULL;

	if (!hstmt || !columns || !values) return MAKE_ERROR(L"Invalid statement object!");

	records = RL_MAKE_BLOCK(128); //GC'ed by REBOL
	if (records == NULL) return MAKE_ERROR(L"Couldn't allocate rows buffer!");

	rc = SQLNumResultCols(hstmt, &num_columns);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return ODBC_ReturnError(frm, SQL_HANDLE_STMT, hstmt);

	while (SQLFetch(hstmt) != SQL_NO_DATA)                                      // Fetch columns
	{
		record = RL_MAKE_BLOCK(num_columns);
		if (record == NULL) return MAKE_ERROR(L"Couldn't allocate record block!");

		for (col = 0; col <= num_columns - 1; col++)
		{
			column     = &columns[col];
			rebol_type = ODBC_ConvertSqlToRebol(column);

			RL_SET_VALUE(record, col, column->value, rebol_type);
		}

		value.series = record;
		value.index  = 0;
		RL_SET_VALUE(records, row++, value, RXT_BLOCK);
	}

	RXA_SERIES(frm, 1) = records;
	RXA_INDEX (frm, 1) = 0;
	RXA_TYPE  (frm, 1) = RXT_BLOCK;
	return RXR_VALUE;
}




