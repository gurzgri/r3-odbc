REBOL [
    title:   "ODBC Open Database Connectivity Scheme"

    name:    odbc
    type:	 module

    options: [extension delay]

    version: 0.6.0
    date:    24-01-2011

    author:  "Christian Ensel"
    rights:  "Copyright (C) 2010-2011 Christian Ensel"

    license: {
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
    }
]

export flatten:  command [block [block!] /deep]

open-connection: command [connection [object!] spec      [string!]]
open-statement:  command [connection [object!] statement [object!]]
insert-odbc:     command [statement  [object!] sql [block!]]
copy-odbc:       command [statement  [object!]]
close-odbc:      command [connection [object! none!] statement [object! none!]]
update-odbc:     command [connection [object!] access [logic!] commit [logic!]]

database-prototype: context [
    environment:        ; henv handle!
    connection:  none   ; hdbc handle!
    statements:  []     ; statement objects
]

statement-prototype: context [
    database:           ;
    statement:          ;
    string:
    titles:
    columns:
    values: none
]

sys/make-scheme [
    name:  'odbc
    title: "ODBC Open Database Connectivity Scheme"

    actor: context [

        ;--------------------------------------------------------------- open --
        ;
        ;   OPEN opens a database port specified as a DSN name (word! syntax)
        ;   or a DSN-less datasource string (block syntax).
        ;
        open: funct [port [port!]] [

            port/state:  context [access: 'write commit: 'auto] ;defaults
            port/locals: make database-prototype []

            result: open-connection port/locals case [
                string? spec: select port/spec 'target [spec]
                string? spec: select port/spec 'host   [ajoin ["dsn=" spec]]

                cause-error 'access 'invalid-spec port/spec
            ]

            all [block? result lit-word? first result apply :cause-error result]    ; not a nice way to return an error from a command ...

            port
        ]


        ;--------------------------------------------------------------- pick --
        ;
        ;   Only here to support FIRST on database ports
        ;
        pick: funct [port [port!] index] [

            set in statement: make statement-prototype [] 'database database: port/locals

            result: open-statement database statement
            all [block? result lit-word? first result apply :cause-error result]    ; not a nice way to return an error from a command ...

        ;   set in port: system/contexts/system/open port/spec/ref 'locals statement
            set in port: lib/open port/spec/ref 'locals statement

            append statement/database/statements port

            port
        ]


        ;------------------------------------------------------------- update --
        ;
        update: funct [port [port!]] [

            if get in connection: port/locals 'connection [
                update-odbc connection port/state/access = 'write port/state/commit = 'auto
                return port
            ]
        ]

        ;-------------------------------------------------------------- close --
        ;
        ;   Closes a statement port only or a database port along with all its
        ;   statement ports.
        ;
        close: funct [port [port!]] [

            if get in statement: port/locals 'statement [
                remove find head statement/database/statements port
                close-odbc none statement
                set words-of statement none
                return port
            ]

            if get in connection: port/locals 'connection [
                while [stmt: first connection/statements] [close stmt]
                close-odbc connection none
                set words-of connection none
                clear connection/statements
                return port
            ]
        ]

        ;------------------------------------------------------------- insert --
        ;
        ;   Insert SQL statements into a statement port. Block arguments
        ;   will be reduced first.                                                  ; probably a design thing to discuss
        ;
        insert: funct [port [port!] sql [string! word! block!]] [
            result: insert-odbc port/locals reduce compose [(sql)]

            all [block? result lit-word? first result apply :cause-error result]    ; not a nice way to return an error from a command ...
            result
        ]

        ;--------------------------------------------------------------- copy --
        ;
        copy: funct [port [port!]] [
            result: copy-odbc port/locals

            all [block? result lit-word? first result apply :cause-error result]    ; not a nice way to return an error from a command ...
            result
        ]
    ]
]


;----------------------------------------------------------- odbc error codes --
;

unprotect system/catalog/errors
extend system/catalog/errors 'ODBC make object! [
    code: system/catalog/errors/access/code + 50
    type: "ODBC error"
    error: [arg1]
]
protect system/catalog/errors
