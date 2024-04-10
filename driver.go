package redshiftdatasqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

const DriverName = "redshift-data"

func init() {
	sql.Register(DriverName, &redshiftDataDriver{})
}

type redshiftDataDriver struct{}

func (d *redshiftDataDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *redshiftDataDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &redshiftDataConnector{
		d:   d,
		cfg: cfg,
	}, nil
}
