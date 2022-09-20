package main

import (
	"flag"
	"fmt"
	"github.com/cavaliergopher/grab/v3"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"time"
)

var schema = `
	create table year(
	    year text,
	    active integer
	);

	create table month(
	    month text,
	    active integer 
	);

	create table file_type (
	    file_type string,
	    name string,
	    active integer
	);

	create table load_control (
	    year text,
	    month text,
	    file_type text,
	    download_url text,
	    filename text,
	    status integer,
	    load_date timestamp,
	    proc_date timestamp,
	    description text
	);

	create table parameter (
	    parameter text,
	    value text,
	    active integer
	);
`

var drops = `
drop table year;
drop table month;
drop table file_type;
drop table load_control;
drop table parameter;
`

var queryFiles = `
	select  p.value || f.file_type|| m.month  || y.year || ".xls" download_url,
			f.file_type|| m.month  || y.year || ".xls" as filename,
			y.year, m.month, f.file_type, p.value as url
	from    file_type f cross join year y cross join month m cross join  parameter p
	where   f.active = 1
	and     y.active = 1
	and     m.active = 1
	and     p.parameter = 'DOWNLOAD_URL_PREFIX'
	order by f.file_type, y.year, m.month;
`

type Year struct {
	Year   string `db:"year"`
	Active bool   `db:"active"`
}

type Month struct {
	Month  string `db:"month"`
	Active bool   `db:"active"`
}

type FileType struct {
	FileType string `db:"file_type"`
	Name     string `db:"name"`
	Active   bool   `db:"active"`
}

type LoadControl struct {
	Year        string    `db:"year"`
	Month       string    `db:"month"`
	FileType    string    `db:"file_type"`
	DownloadURL string    `db:"download_url"`
	Filename    string    `db:"filename"`
	Status      int       `db:"status"`
	LoadDate    time.Time `db:"load_date"`
	ProcDate    time.Time `db:"proc_date"`
	Description string    `db:"description"`
}

type Parameter struct {
	Parameter string `db:"parameter"`
	Value     string `db:"value"`
	Active    bool   `db:"active"`
}

func init() {
}

func main() {
	db, err := sqlx.Connect("sqlite3", "file:utec_load_control.db?cache=shared&mode=rwc")
	defer db.Close()
	if err != nil {
		log.Fatalf("error conectando con la base de datos de control [%v]", err.Error())
	}

	var initDb bool
	var dropTables bool
	var seed bool
	flag.BoolVar(&initDb, "init", false, "Initialize database creating the tables")
	flag.BoolVar(&dropTables, "drop", false, "Drop all the tables")
	flag.BoolVar(&seed, "seed", false, "Seed the database with default data")
	flag.Parse()

	log.Println("Flags :")
	log.Printf("	- initDB 		-> [%v]\n", initDb)
	log.Printf("	- dropTables		-> [%v]\n", dropTables)
	log.Printf("	- seed 			-> [%v]\n", seed)

	if dropTables {
		db.MustExec(drops)
		os.ReadDir("./files")

	}

	if initDb {
		// create schema
		db.MustExec(schema)
	}

	if seed {
		// fill Schema with data
		tx := db.MustBegin()
		// years
		for y := 4; y <= 22; y++ {
			tx.MustExec("insert into year (year, active) values ($1, $2)", fmt.Sprintf("%02d", y), 1)
		}

		// months
		for m := 1; m <= 12; m++ {
			tx.MustExec("insert into month (month, active) values ($1, $2)", fmt.Sprintf("%02d", m), 1)
		}

		// file types
		tx.MustExec("insert into file_type(file_type, name, active) values ($1, $2, $3)", "dg_sa_", "Saldo adeudado por departamento", 1)
		tx.MustExec("insert into file_type(file_type, name, active) values ($1, $2, $3)", "s_sa_", "Saldo adeudado", 1)
		tx.MustExec("insert into file_type(file_type, name, active) values ($1, $2, $3)", "s_mo_", "Montos otorgados", 1)
		tx.MustExec("insert into file_type(file_type, name, active) values ($1, $2, $3)", "s_csa_", "Saldo adeudado por categoría de riesgo", 1)
		tx.MustExec("insert into file_type(file_type, name, active) values ($1, $2, $3)", "s_cmo_", "Monto otorgado por categoría de riesgo", 1)

		// parameters
		tx.MustExec("insert into parameter(parameter, value, active) values ($1, $2, $3)", "DOWNLOAD_URL_PREFIX", "https://ssf.gob.sv/descargas/balances/xls/", 1)

		// commit
		err = tx.Commit()
		if err != nil {
			log.Fatalf("Seed cannot be executed - error [%v]", err.Error())
		}

	}

	log.Println("Inicializando descargas")
	rows, err := db.Queryx(queryFiles)
	if err != nil {
		log.Fatalf("error extrayendo nombres de archivos [%v]", err.Error())
	}

	for rows.Next() {
		var downloadUrl string
		var filename string
		var year string
		var month string
		var fileType string
		var url string

		err = rows.Scan(&downloadUrl, &filename, &year, &month, &fileType, &url)
		log.Printf("... nombre de archivo a descargar -> %s", filename)

		resp, err := grab.Get("./files", downloadUrl)
		if err != nil {
			db.MustExec(`insert into load_control(year, month, file_type, download_url, filename, status, load_date, description) 
							values ($1, $2, $3, $4, $5, $6, $7, $8)`,
				year,
				month,
				fileType,
				downloadUrl,
				filename,
				-1,
				time.Now(),
				fmt.Sprintf("Descarga de archivos en error [%v]", err.Error()))
		}

		db.MustExec(`insert into load_control(year, month, file_type, download_url, filename, status, load_date, description) 
						values ($1, $2, $3, $4, $5, $6, $7, $8)`,
			year,
			month,
			fileType,
			downloadUrl,
			filename,
			1,
			time.Now(),
			fmt.Sprintf("Descarga de archivos satisfactoria bytes [%v]", resp.Size()))

	}

}
