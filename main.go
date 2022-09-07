package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xo/dburl"
	"os"
	"sync"
	"time"
)

var (
	log = logging.Logger("exporter")
	wg  sync.WaitGroup

	outFile string
	dbURL   string
	debug   bool
)

func init() {
	flag.StringVar(&outFile, "out-file", "deals.txt", "filename to write")
	flag.StringVar(&dbURL, "db-url", "sqlite:db.sqlite3?loc=auto", "database connection info")
	flag.BoolVar(&debug, "debug", false, "enable debug model")
}

func main() {
	flag.Parse()
	if debug {
		logging.SetDebugLogging()
		logging.SetLogLevel("rpc", "info") // nolint: errcheck
	} else {
		logging.SetLogLevel("*", "info")
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	ai := ParseApiInfo(os.Getenv("FULLNODE_API_INFO"))

	addr, err := ai.DialArgs("v1")
	if err != nil {
		log.Error(err)
		return
	}

	node, closer, err := client.NewFullNodeRPCV1(ctx, addr, ai.AuthHeader())
	if err != nil {
		log.Errorf("connect to lotus rpc: %s", err)
		return
	}
	defer closer()

	start := time.Now()
	deals, err := node.StateMarketDeals(ctx, types.EmptyTSK)
	if err != nil {
		log.Errorf("get deals: %s", err)
		return
	}
	log.Infow("get deals", "took", time.Since(start).String())

	wg.Add(1)
	go func() {
		start = time.Now()
		defer func() {
			log.Infow("write deals to file", "took", time.Since(start).String())
		}()
		log.Infof("writing deals to %s", outFile)
		if err := writeToTxt(outFile, deals); err != nil {
			log.Errorf("write deals to file: %s", err)
		}
	}()

	wg.Add(1)
	go func() {
		start := time.Now()
		defer func() {
			log.Infow("write to db", "took", time.Since(start).String())
		}()
		log.Infof("writing to db...")
		if err := writeToDB(ctx, dbURL, deals); err != nil {
			log.Errorf("write to db: %s", err)
		}
	}()
	wg.Wait()
}

func writeToTxt(file string, deals map[string]*api.MarketDeal) error {
	defer wg.Done()
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close() // nolint: errcheck
	for id, deal := range deals {
		v, err := json.Marshal(deal)
		if err != nil {
			return err
		}
		f.Write([]byte(id)) // nolint: errcheck
		f.WriteString("|")  // nolint: errcheck
		f.Write(v)          // nolint: errcheck
		f.WriteString("\n") // nolint: errcheck
	}
	return nil
}

func writeToDB(ctx context.Context, url string, deals map[string]*api.MarketDeal) error {
	defer wg.Done()
	db, err := dburl.Open(url)
	if err != nil {
		return err
	}
	defer db.Close() // nolint: errcheck
	log.Infof("creating db tables...")
	if err := CreateTables(ctx, db); err != nil {
		return err
	}

	var (
		dds   []*DealModel
		count int64
	)

	dd := NewDealsDB(db)
	for id, deal := range deals {
		if len(dds) >= 500 {
			err := dd.Insert(ctx, dds)
			if err != nil {
				return err
			}
			dds = []*DealModel{}
		}
		dds = append(dds, &DealModel{
			ID:         id,
			MarketDeal: *deal,
		})
		count++
		if count%100 == 0 {
			log.Debugw("store to db", "count", count, "id", id)
		}
	}
	if len(dds) > 0 {
		err := dd.Insert(ctx, dds)
		if err != nil {
			return err
		}
	}
	return nil
}
