package database

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"time"
)

const (
	DriverMysql = "mysql"
)

var (
	Conn     *DB
	dbTicker *time.Ticker
)

type (
	DSNConfig struct {
		DSN string
	}

	DBConfig struct {
		ConnDSN         string `json:"conn_dsn" mapstructure:"conn_dsn"`
		RetryInterval   int    `json:"retry_interval" mapstructure:"retry_interval"`
		MaxIdleConn     int    `json:"max_idle" mapstructure:"max_idle"`
		MaxConn         int    `json:"max_con" mapstructure:"max_con"`
		ConnMaxLifetime string `json:"conn_max_lifetime" mapstructure:"conn_max_lifetime"`
	}

	DB struct {
		DBConnection    *sqlx.DB
		DBString        string
		RetryInterval   int
		MaxIdleConn     int
		MaxConn         int
		ConnMaxLifetime time.Duration
		doneChannel     chan bool
	}

	Store struct {
		Conn *sqlx.DB
	}

	Options struct {
		dbTx *sqlx.Tx
	}
)

func (s *Store) GetConn() *sqlx.DB {
	return s.Conn
}

func (d *DB) Connect(driver string) error {
	db, err := sqlx.Open(driver, d.DBString)
	if err != nil {
		return fmt.Errorf("failed to open DB connection: %w", err)
	}

	db.SetMaxOpenConns(d.MaxConn)
	db.SetMaxIdleConns(d.MaxIdleConn)

	if d.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(d.ConnMaxLifetime)
	}

	d.DBConnection = db

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping DB: %w", err)
	}

	return nil
}

func (d *DB) ConnectAndMonitor(driver string) error {
	err := d.Connect(driver)

	if err != nil {
		log.Printf("Not connected to database %s, trying", d.DBString)
		return err
	}

	ticker := time.NewTicker(time.Duration(d.RetryInterval) * time.Second)
	go func() error {
		for {
			select {
			case <-ticker.C:
				if d.DBConnection == nil {
					d.Connect(driver)
				} else {
					err := d.DBConnection.Ping()
					if err != nil {
						log.Println("[Error]: DB reconnect error", err.Error())
						return err
					}
				}
			case <-d.doneChannel:
				return nil
			}
		}
	}()
	return nil
}


func New(cfg DBConfig, dbDriver string) *Store {
	connDSN := cfg.ConnDSN

	var conMaxLifetime time.Duration
	if cfg.ConnMaxLifetime != "" {
		duration, err := time.ParseDuration(cfg.ConnMaxLifetime)
		if err != nil {
			log.Fatal("Invalid ConnMaxLifetime value: " + err.Error())
			return &Store{}
		}

		conMaxLifetime = duration
	}

	Conn = &DB{
		DBString:        connDSN,
		RetryInterval:   cfg.RetryInterval,
		MaxIdleConn:     cfg.MaxIdleConn,
		MaxConn:         cfg.MaxConn,
		ConnMaxLifetime: conMaxLifetime,
		doneChannel:     make(chan bool),
	}

	err := Conn.ConnectAndMonitor(dbDriver)
	if err != nil {
		log.Fatal("Could not initiate DB connection: " + err.Error())
		return &Store{}
	}

	dbTicker = time.NewTicker(time.Second * 2)
	return &Store{Conn: Conn.DBConnection}
}

