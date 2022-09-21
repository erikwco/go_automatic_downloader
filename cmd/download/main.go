package main

import (
	"flag"
	"fmt"
	"github.com/cavaliergopher/grab/v3"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"sync"
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
	order by f.file_type, y.year, m.month ;
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

// **************************************************************
// Worker section
// **************************************************************

// Job representa cada archivo descargado o procesado
type Job struct {
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

type JobChannel chan Job
type JobQueue chan chan Job

// Worker representa a un simple procesador, lo ideal es levantar
// mas de uno ya que es el que hace el trabajo
type Worker struct {
	Id      int           // Id del worker
	JobChan JobChannel    // Canal que recibe una unidad de trabajo
	Queue   JobQueue      // Canal compartido entre los workers
	Quit    chan struct{} // Controla el fin del trabajo
	Db      *sqlx.DB
	Wg      *sync.WaitGroup
}

func NewWorker(Id int, jobChan JobChannel, queue JobQueue, quit chan struct{}, db *sqlx.DB, wg *sync.WaitGroup) *Worker {
	return &Worker{
		Id:      Id,
		JobChan: jobChan,
		Queue:   queue,
		Quit:    quit,
		Db:      db,
		Wg:      wg,
	}
}

func (w *Worker) Start() {
	go func() {
		for {
			// El canal de jobs se ingresa a la cola al estar disponible
			w.Queue <- w.JobChan
			select {
			case job := <-w.JobChan:
				w.Wg.Add(1)
				// procesar el trabajo recibido
				downloader(w.Db, job, w.Wg)
			case <-w.Quit:
				// se recibió señal de cierre
				close(w.JobChan)
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	close(w.Quit)
}

// **************************************************************
// Worker section
// **************************************************************

// **************************************************************
// Dispatcher section
// **************************************************************

type Dispatcher struct {
	Workers  []*Worker  // Listado de workers
	WorkChan JobChannel // Trabajo enviado por el cliente
	Queue    JobQueue   // Pool compartido entre los Worker
	Db       *sqlx.DB
	Wg       sync.WaitGroup
}

// NewDispatcher retorna un Dispatcher que es el canal de comunicación entre
// el cliente y los workers, su trabajo principal es recibir nuevos
// trabajos y compartirlo con el pool
func NewDispatcher(num int, db *sqlx.DB) *Dispatcher {
	var wg sync.WaitGroup
	return &Dispatcher{
		Workers:  make([]*Worker, num),
		WorkChan: make(JobChannel),
		Queue:    make(JobQueue),
		Db:       db,
		Wg:       wg,
	}
}

// Start inicializa los diferentes workers
// llamando a Start de cada worker para que este
// pendiente del trabajo
func (d *Dispatcher) Start() *Dispatcher {
	// workers creados
	l := len(d.Workers)
	// Iniciando workers
	for i := 1; i <= l; i++ {
		wrk := NewWorker(i, make(JobChannel), d.Queue, make(chan struct{}), d.Db, &d.Wg)
		wrk.Start()
		d.Workers = append(d.Workers, wrk)
	}
	//  esperando por trabajo
	go d.process()
	return d
}

// process escucha los trabajos enviados al canal
// y lo ingresa al Pool. El pool es compartido
// entre los workers
func (d *Dispatcher) process() {
	for {
		select {
		case job := <-d.WorkChan: // esperando el nuevo trabajo ingresado
			// espera que se reciba un nuevo job
			// y esta línea espera que exista un worker libre
			jobChan := <-d.Queue

			// enviamos el job al canal
			jobChan <- job
		}
	}
}

func (d *Dispatcher) Submit(job Job) {
	d.WorkChan <- job
}

// **************************************************************
// Dispatcher section
// **************************************************************

func main() {
	db, err := sqlx.Connect("sqlite3", "file:utec_load_control.db?cache=shared&mode=rwc")
	defer db.Close()
	if err != nil {
		log.Fatalf("error conectando con la base de datos de control [%v]", err.Error())
	}

	var initDb bool
	var dropTables bool
	var seed bool
	var deleteFiles bool
	var downloadFiles bool
	flag.BoolVar(&initDb, "init", false, "Initialize database creating the tables")
	flag.BoolVar(&dropTables, "drop", false, "Drop all the tables")
	flag.BoolVar(&seed, "seed", false, "Seed the database with default data")
	flag.BoolVar(&deleteFiles, "del-files", false, "Delete the files previous to download")
	flag.BoolVar(&downloadFiles, "download", false, "Download all the configured files")
	flag.Parse()

	log.Println("Flags :")
	log.Printf("	- initDB 		-> [%v]\n", initDb)
	log.Printf("	- dropTables		-> [%v]\n", dropTables)
	log.Printf("	- seed 			-> [%v]\n", seed)
	log.Printf("	- del-files		-> [%v]\n", deleteFiles)
	log.Printf("	- download		-> [%v]\n", downloadFiles)

	if deleteFiles {
		fmt.Println("... Eliminando archivos")
		files, err := os.ReadDir("./files")
		if err != nil {
			log.Fatalf("No fue posible obtener listado de archivos a borrar [%v]", err.Error())
		}

		if len(files) > 0 {
			for _, file := range files {
				err := os.Remove(fmt.Sprintf("./files/%s", file.Name()))
				if err != nil {
					fmt.Printf("Error eliminando archivo %s \n", file.Name())
				} else {
					fmt.Printf("Archivo eliminado %s \n", file.Name())
				}
			}
		} else {
			fmt.Println("... Sin archivos que eliminar")
		}

		db.MustExec("delete from load_control where 1=1")

	}

	if dropTables {
		db.MustExec(drops)
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

	if downloadFiles {
		log.Println("Inicializando descargas")
		start := time.Now()
		// query de archivos a extraer
		rows, err := db.Queryx(queryFiles)
		if err != nil {
			log.Fatalf("error extrayendo nombres de archivos [%v]", err.Error())
		}

		//
		dd := NewDispatcher(10, db).Start()

		//
		for rows.Next() {
			lc := Job{}
			var url string
			err = rows.Scan(&lc.DownloadURL, &lc.Filename, &lc.Year, &lc.Month, &lc.FileType, &url)
			dd.Submit(lc)
		}
		//
		end := time.Now()
		log.Printf(">>> Tiempo de ejecución: [%v]", end.Sub(start).Seconds())
		dd.Wg.Wait()
		//time.Sleep(5 * time.Second)
	}

}

func downloader(db *sqlx.DB, lc Job, wg *sync.WaitGroup) {
	defer wg.Done()
	insertLoadControl := `insert into load_control(year, month, file_type, download_url, filename, status, load_date, description) 
							values (:year, :month, :file_type, :download_url, :filename, :status, :load_date, :description)`

	log.Printf("... nombre de archivo a descargar -> %s", lc.Filename)

	resp, err := grab.Get("./files", lc.DownloadURL)
	if err != nil {
		lc.Description = fmt.Sprintf("Descarga de archivos en error [%v]", err.Error())
	} else {
		lc.Description = fmt.Sprintf("Descarga de archivos satisfactoria bytes [%v]", resp.Size())
	}
	lc.LoadDate = time.Now()
	r, err := db.NamedExec(insertLoadControl, lc)
	if err != nil {
		log.Printf("--- No fue posible insertar registro de control  [%v]", err.Error())
		return
	}
	ra, err := r.RowsAffected()
	if err != nil {
		log.Printf("--- No fue posible obtener filas afectadas [%v]", err.Error())
		return
	}
	log.Printf("+++ Registros afectados [%d]", ra)

}
