REBOL3 ODBC Extension

    Version: 0.6.0
    Date:    24-01-2011
    Author:  Christian Ensel


=== Introduction

--- About

The R3 ODBC Extension allows easy access to databases and data sources supporting ODBC.

The extension supports SQL statements <b>select</b> and <b>insert</b>, <b>update</b> and <b>delete</b>
as well as <b>catalog</b> functions for tables, columns and types. It supports <b>statement parameters</b>
and is Unicode aware. It supports <b>direct and prepared execution</b> of statements.

If you're used to the commercial REBOL/Command (or the nowadays freely available REBOL/View) ODBC database access methods,
you should have no problems using the R3 ODBC extension, there's next to no differences in functionality.

The ODBC extension is available as a DLL for the Windows platform. So far it has been tested with MySQL, PostgreSQL and
Intersystems Caché as well as the Microsoft Text Driver. Of course it's supposed to work with any ODBC data source.

--- Installation

Download the <a href="odbc.dll">odbc.dll</a> to a place where your <b>r3.exe</b> can access it.
Then just import the DLL with

    >> import %odbc.dll

In case you experience problems with character sets, try using the DLL with a host built as a Windows GUI application
to get a proper unicode console.


=== Database Connections

--- Opening Connections

If a data source has been set up in the systems ODBC panel, <b>open</b>ing a connection is as easy as

    >> connection: open odbc://datasource-name

To connect to a database using a connection string, use

    >> connection: open [scheme: 'odbc target: "driver={drivername};server=127.0.0.1;port=1972;database=samples;uid=user;pwd=pass"]

with a target string tailored to the specific requirements of the database engine you're using.

--- Opening Statements

After you've successfully connected to the database, you have to allocate one or more statement handles
via the <b>first</b> function:

    >> db: first connection

You may allocate multiple statements on the same database connection:

    >> db1: first connection
    >> db2: first connection
    >> db3: first connection

There are benefits in using multiple statements for specialised purposes depending on your usage patterns, see the section on statement preparation below.

--- Closing Statements and Connections

To close only a statement, do

    >> close db

To close a connection along with all associated statements do

    >> close connection


=== SQL Statements

--- Inserting Statements, retrieving results

The following examples should give an (informal) idea on how SQL statements are executed. Here's is a trivial SELECT statement:

    >> insert db "select * from Cinema.Film"
    == [id category description length playing-now rating tickets-sold title]
    >> copy db
    == [[1 1 {A post-modern excursion into family dynamics and Thai cuisine.} 130 true "PG-13" 47000 "ÄÖÜßäöü"]
        [2 1 "A gripping true story of honor and discovery" 122 true "R" 50000 "A Kung Fu Hangman1"]
        [3 1 "A Jungian analysis of pirates and honor" 101 true "PG" 5000 "A Kung Fu Hangman"]
        [4 1 "A charming diorama about sibling rivalry" 124 true "G" 7000 "Holy Cooking"]
        [5 2 "An exciting diorama of struggle in Silicon Valley" 100 true "PG" 48000 "The Low Calorie Guide to the Internet"]
        [6 2 "A heart-w...

A parametrized SELECT statement:

    >> insert db ["select * from Cinema.Film where ID = ?" 6]
    == [id category description length playing-now rating tickets-sold title]
    >> copy db
    == [[6 2 "A heart-warming tale of friendship" 91 true "G" 7500 "Gangs of New York"]]

A INSERT statement inserting one row:

    >> insert db ["insert into Persons (Name, Age) values (?, ?)" person/name person/age]
    == true
    >> copy db
    == 1

A UPDATE statement updating no rows at all:

    >> insert db ["update Persons set Name = ? where ID = ?" "nobody" 0]
    == true
    >> copy db
    == 0

A DELETE statement deleting five rows:

    >> insert db ["delete from Subscribers where Subscription = ?" cancel]
    == true
    >> copy db
    == 5


--- Affected rows

With row-changing INSERT/UPDATE/DELETE statements, <b>insert</b> simply returns <b>true</b>, if the statement has been executed successfully.
On <b>copy</b> you retrieve the number of rows affected by the statement:

    >> insert db ["insert into Persons (Name, Age) values (?, ?)" person/name person/age]
    == true
    >> copy db
    == 1


--- Column names

For SELECT statements and catalog functions (see below) <b>insert</b> returns a block of column names as REBOL words, while <b>copy</b> retrieves
the actual results of the statement.

Using the column names returned it's easy to keep your rebol code in sync with your SQL statements:

    >> columns: insert statement ["select ID, Category, Title from Cinema.Film"]
    == [id category title]
    >> foreach :columns flatten copy db [print [id category title]]
    1 1 ÄÖÜßäöü
    2 1 A Kung Fu Hangman1
    3 1 A Kung Fu Hangman
    4 1 Holy Cooking
    5 2 The Low Calorie Guide to the Internet
    ...

If, for some reason, you later change your SQL statement to something like

    >> columns: insert db ["select ID, Descriptions, Title, Length, Category from cinema.film"]
    == [id description title length category]

this will work without modifications with the same retrieval code as above, requiring no changes at all:

    >> foreach :columns flatten copy db [print [id category title]]
    1 1 ÄÖÜßäöü
    2 1 A Kung Fu Hangman1
    3 1 A Kung Fu Hangman
    4 1 Holy Cooking
    5 2 The Low Calorie Guide to the Internet

The column names are generated directly from the result set's column description and made available as normal rebol words.
There is some "magic" involved which transforms names to adhere to REBOL standards of lowercase hyphenated words instead of, say, "CamelCaseWords".

The rules are fairly simple:

*"ABc" becomes 'a-bc,

*"aB" becomes 'a-b,

*the underscore "_" is replaced with a hyphen '-

*all uppercase letters are converted to lowercase

Some examples:

    SQL names:   ID FirstName  isValid  CamelCaseABC   ODBCTest  Table_ID Expr_1 A1B2
    REBOL words: id first-name is-valid camel-case-abc odbc-test table-id expr-1 a1b2


--- Prepared Statements

Often, you'll find yourself executing the same SQL statements again and again. The ODBC extension supports this
by preparing statements for later reuse (i.e. execution), which saves the ODBC driver and your database the effort
to parse the SQL and to determine an access plan every single time. Instead, a previously prepared statement is reused and no
statement string needs to be transfered to the database.

To prepare a statements, just <b>insert</b> a SQL string, likely along with parameter markers <b>?</b> and parameters.
If later you <b>insert</b> the same SQL string along with that statement, internaly the statement is only excecuted, not prepared again.

Successive calls to <b>insert</b> then supply the same SQL, paramaters, however, may of course differ:

    >> db: first database: open odbc://mydatabase
    >> sql: "select * from Table where Value = ?"
    >> insert db [sql 1]
    >> insert db [sql 2]
    >> insert db [sql 3]
    >> close db

The more complex your statement is, the more noticable the speed gain achievable with prepared statements should get.

Whether a SQL string supplied needs to be prepared before execution or wheter it can be excecuted right away, is determined by the SQL string.

    >> db: first database: open odbc://mydatabase
    >> sql-a: "select * from Table where Value = ?"
    >> insert db [sql-a 1] ;preparation and execution
    >> insert db [sql-a 2] ;execution only
    >> sql-b: "select * from Table where Value > ?"; SQL-A now is a new string
    >> insert db [sql-b 3] ;preparation and execution
    >> insert db [sql-b 4] ;execution only
    >> insert db [sql-a 1] ;again, preparation and execution



--- Flatten Function

Sometimes you may want to retrieve results as flat values instead of record blocks. You can do so with <bBflatten</b>:

    >> insert db "select * from cinema.show" copy db
    == [[1 1 12:00 7]
        [2 1 15:45 7]
        [3 1 17:00 7]
        [4 1 19:30 7]
        [5 1 12:...
    >> insert db "select * from cinema.show" flatten copy db
    == [1 1 12:00 7 2 1 15:45 7 3 1 17:00 7 4 1 19:30 7 5 1 12:25 8 6 1 14:55 8 7 1 17:25
        8 8 1 19:55 8 9 1 12:25 9 10 1 14:55 9 11 1 17:25 9 12 1 19:55 9 13 2 12:1 0 3 14
        2 14:30 3 15 2 16:50 3 16 2 19:10 3 17 2 21:30 3 18 2 12:05 4 19 2 14:25 4 20 ...

The <b>flatten</b> function comes along with the ODBC extension but may prove generally useful in other situations, too:

    >> flatten [1 2 3 4 5]
    == [1 2 3 4 5]
    >> flatten [1 [2 [3 [4 [5]]]]]
    == [1 2 [3 [4 [5]]]]
    >> flatten/deep [1 [2 [3 [4 [5]]]]]
    == [1 2 3 4 5]


--- Statement Parameters

You may already have noticed the use of statement parameters. To use them, instead of just supplying a statement string supply
a block to <b>insert</b>. The statement string has to be the first item in the block, let parameters follow as applicable:

    >> insert db ["select ? from Sample.Table where ID = ?" "Test" 2]
    >> copy db
    == [["Test"]]

Note that the block supplied will be reduced automatically:

    >> set [name age] ["Homer Simpson" 49]
    >> insert db ["insert into Persons (Name, Age) values (?, ?)" name age]
    >> copy db
    == 1

The datatypes supported so far are:

*integer!

*decimal!

*logic!

*time!

*date! (currently as strings only, waiting for a108 to implement this)

*string!

*binary!


--- Datatype Conversions

If the built in automatic type conversion for data retrieval doesn't fit your needs, you may cast values to different types
in your SQL statement:

    >> insert db ["select 1 * ID from Sample.Person where ID = 1"] type? first copy db
    == decimal!
    >> insert db ["select cast (1 * ID as integer) from Sample.Person where ID = 1"] type? first copy db
    == integer!

Statement parameters inserted into the result columns will always be returned as strings unless told otherwise:

    >> insert db ["select ? from Sample.Person where ID = 1" 1] type? first copy db
    == string!
    >> insert db ["select cast (? as integer) from Sample.Person where ID = ?" 1] type? first copy db
    == integer!

If there is no applicable REBOL datatype to contain a SQL value, the value will be returned as a string.


--- Catalog functions

+++ Tables

To learn about the tables in a database, you can

    >> insert db ['tables]
    >> insert db ['tables "Person"]
    >> insert db ['tables "Person" "Company"]
    >> insert db ['tables none     "Company"]

The general dialect syntax is:

    >> insert db ['tables <table-name> <schema-name> <catalog-name> <table-type>]

The <b>'tables</b> lit-word returns the tables asked for as a result set.

+++ Columns

To learn about the columns of your databases' tables, do

    >> insert db ['columns "Person"]
    >> insert db ['columns "Person" "%Name%"]
    >> insert db ['columns "Person" none     "Company"]
    >> insert db ['columns none     "ID"     "Company"]

The general dialect for column retrieval is:

    do-sql statement ['tables <table-name> <column-name> <schema-name> <catalog-name>]

The <b>'columns</b> lit-word returns the columns asked for as a result set.

+++ Types

To learn about the datatypes supported by the data source, use

    >> insert db ['types]

The <b>'types</b> lit-word returns the datatypes supported by the connected data source as a result set.


=== License

Author: Christian Ensel

Rights: Copyright (C) Christian Ensel 2010-2011

This software is provided 'as-is', without any express or implied warranty.
In no event will the author be held liable for any damages arising from the
use of this software.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.



