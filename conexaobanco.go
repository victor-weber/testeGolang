package main

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func conectaBanco() (db *sqlx.DB, err error) {
	/*************************
	** Abre banco de dados **
	*************************/
	dbsource := "postgres://postgres:teste@localhost:5432/teste-postgres?sslmode=disable"
	//dbsource := "postgres://postgres@localhost/testdb?sslmode=disable"
	db, err = open(dbsource)
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}

func open(dbsource string) (db *sqlx.DB, err error) {
	db, err = sqlx.Open("postgres", dbsource)
	if err != nil {
		err = fmt.Errorf("error open db: %v", err)
		return
	}

	err = db.Ping()
	if err != nil {
		err = fmt.Errorf("error ping db: %v", err)
	}
	return
}
