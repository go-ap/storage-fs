package cmd

import (
	"fmt"
	"github.com/go-ap/auth"
	"github.com/go-ap/errors"
	"github.com/go-ap/fedbox/internal/config"
	"gopkg.in/urfave/cli.v2"
	"os"
)

var BootstrapCmd = &cli.Command{
	Name:  "bootstrap",
	Usage: "Bootstrap a new postgres or bolt database helper",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "root",
			Usage: "root account of postgres server (default: postgres)",
			Value: "postgres",
		},
		&cli.StringFlag{
			Name:  "sql",
			Usage: "path to the queries for initializing the database",
			Value: "postgres",
		},
	},
	Action: bootstrapAct(&ctl),
	Subcommands: []*cli.Command{
		reset,
	},
}

var reset = &cli.Command{
	Name:   "reset",
	Usage:  "reset an existing database",
	Action: resetAct(&ctl),
}

func resetAct(c *Control) cli.ActionFunc {
	return func(ctx *cli.Context) error {
		err := bootstrapReset(c.Conf)
		if err != nil {
			return err
		}
		return bootstrap(c.Conf)
	}
}

func bootstrapAct(c *Control) cli.ActionFunc {
	return func(ctx *cli.Context) error {
		return bootstrap(c.Conf)
	}
}

func bootstrapOAuth(conf config.Options) error {
	if conf.Storage == config.StorageFS{
		return nil
	}
	oauthPath := config.GetDBPath(conf.StoragePath, fmt.Sprintf("%s-oauth", conf.Host), conf.Env)
	if _, err := os.Stat(oauthPath); os.IsNotExist(err) {
		err = auth.BootstrapBoltDB(oauthPath, []byte(conf.Host))
		if err != nil {
			return errors.Annotatef(err, "Unable to create %s db", oauthPath)
		}
	}
	return nil
}

func bootstrap(conf config.Options) error {
	if err := bootstrapFn(conf); err != nil {
		return errors.Annotatef(err, "Unable to create %s db for storage %s", conf.StoragePath, conf.Storage)
	}
	return bootstrapOAuth(conf)
}

func bootstrapReset(conf config.Options) error {
	return cleanFn(conf)
}
