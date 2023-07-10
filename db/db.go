package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/denuoweb/ethereum-block-processor/jsonrpc"
	"github.com/denuoweb/ethereum-block-processor/log"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

type DbConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSL      bool
}

func (config DbConfig) String() string {
	ssl := "disable"
	if config.SSL {
		ssl = "enable"
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", config.Host, config.Port, config.User, config.Password, config.DBName, ssl)
}

type HtmlcoinDB struct {
	db           *sql.DB
	logger       *logrus.Entry
	records      int64
	resultChan   chan jsonrpc.HashPair
	shutdownChan chan struct{}
	errChan      chan error
	running      bool
	mutex        sync.RWMutex
}

func NewHtmlcoinDB(ctx context.Context, connectionString string, resultChan chan jsonrpc.HashPair, errChan chan error) (*HtmlcoinDB, error) {
	dbLogger, _ := log.GetLogger()
	logger := dbLogger.WithField("module", "db")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	logger.Debug("Database Connected!")
	createHashes := `CREATE TABLE IF NOT EXISTS "Hashes" ("BlockNum" int, "ChainId" int, "Eth" text, "Htmlcoin" text NOT NULL, PRIMARY KEY("Eth", "ChainId"))`
	_, err = db.ExecContext(ctx, createHashes)

	if err != nil {
		return nil, errors.WithMessage(err, "Failed to create 'Hashes' table")
	}

	return &HtmlcoinDB{db: db, logger: logger, resultChan: resultChan, shutdownChan: make(chan struct{}), errChan: errChan}, nil
}

func (q *HtmlcoinDB) insert(ctx context.Context, blockNum, chainID int, eth, htmlcoin string) (sql.Result, error) {
	if chainID == 0 {
		panic(chainID)
	}
	insertDynStmt := `INSERT INTO "Hashes"("BlockNum", "ChainId", "Eth", "Htmlcoin") VALUES($1, $2, $3, $4) ON CONFLICT ON CONSTRAINT "Hashes_pkey" DO UPDATE SET "Htmlcoin" = $3`
	return q.db.ExecContext(ctx, insertDynStmt, blockNum, chainID, eth, htmlcoin)
}

func (q *HtmlcoinDB) GetMissingBlocks(ctx context.Context, chainId int, latestBlock int64) ([]int64, error) {
	offset := 0
	limit := 500000
	missingBlocks := make([]int64, latestBlock+int64(limit))
	results := 0

	for {
		result, nextOffset, err := q.GetMissingBlocksRange(ctx, chainId, latestBlock, limit, offset)
		if err != nil {
			return nil, err
		}
		if len(result) == 0 {
			return missingBlocks[0:results], nil
		}

		// copy results into missing blocks
		for i := 0; i < len(result)-1; i++ {
			missingBlocks[offset+i] = result[i]
			results++
		}

		offset = nextOffset
	}
}

func (q *HtmlcoinDB) GetMissingBlocksRange(ctx context.Context, chainId int, latestBlock int64, limit, offset int) ([]int64, int, error) {
	// takes 1.5 sec for 2m rows on local postgres dev instance
	missing := `
	SELECT "B"."BlockNum"
	FROM "Hashes" AS "A"
	RIGHT JOIN (select generate_series(1, $1) AS "BlockNum", $2::int4 As "ChainId") AS "B"
	ON "A"."BlockNum" = "B"."BlockNum"
    AND "A"."ChainId" = "B"."ChainId"
	WHERE "A"."BlockNum" IS NULL
    LIMIT $3 OFFSET $4
	`
	rows, err := q.db.QueryContext(ctx, missing, latestBlock, chainId, limit, offset)

	if err != nil {
		return nil, offset, err
	}

	result := make([]int64, limit)
	rowCount := 0

	for ; rows.Next(); rowCount++ {
		err = rows.Scan(&result[rowCount])
		if err != nil {
			return nil, offset, err
		}
	}

	return result[0:rowCount], limit + offset, nil
}

func (q *HtmlcoinDB) getHtmlcoinHash(ctx context.Context, chainId int, ethHash string) (*sql.Rows, error) {
	selectStatement := `SELECT "Htmlcoin" FROM "Hashes" WHERE "Hashes"."ChainId" = $1 AND "Hashes"."Eth" = $2`
	if ctx == nil {
		return q.db.Query(selectStatement, chainId, ethHash)
	} else {
		return q.db.QueryContext(ctx, selectStatement, chainId, ethHash)
	}
}

func (q *HtmlcoinDB) GetHtmlcoinHash(chainId int, ethHash string) (*string, error) {
	return q.GetHtmlcoinHashContext(nil, chainId, ethHash)
}

func (q *HtmlcoinDB) GetHtmlcoinHashContext(ctx context.Context, chainId int, ethHash string) (*string, error) {
	var htmlcoinHash string
	rows, err := q.getHtmlcoinHash(ctx, chainId, ethHash)
	if err != nil {
		return &htmlcoinHash, err
	}

	if rows == nil {
		panic("no rows")
	}

	if !rows.Next() {
		return nil, nil
	}

	err = rows.Scan(&htmlcoinHash)
	return &htmlcoinHash, err
}

func (q *HtmlcoinDB) Shutdown() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.shutdownChan <- struct{}{}
}

func (q *HtmlcoinDB) Start(ctx context.Context, chainId int, dbCloseChan chan error) {
	q.mutex.Lock()
	if q.running {
		q.mutex.Unlock()
		return
	}
	q.running = true
	q.mutex.Unlock()
	const PROGRESS_LEVEL_THRESHOLD = 10000
	go func() {
		progBar := getBar(PROGRESS_LEVEL_THRESHOLD)
		start := time.Now()
		defer func() {
			q.logger.Debug("database exiting")
			q.mutex.Lock()
			q.running = false
			q.mutex.Unlock()
		}()

		shuttingDown := false

		for {
			q.logger.Info("Waiting for results...")
			var pair jsonrpc.HashPair
			var ok bool

			if shuttingDown {
				select {
				case pair, ok = <-q.resultChan:
				default:
					// shutdown, finished draining results
					q.logger.Info("Database finished draining results, shutting down")
					err := q.db.Close()
					dbCloseChan <- err
					return
				}
			} else {
				select {
				case pair, ok = <-q.resultChan:
				case <-ctx.Done():
					shuttingDown = true
					continue
				case <-q.shutdownChan:
					// finish writing then shutdown
					shuttingDown = true
					continue
				}
			}

			if !ok {
				q.logger.Info("HtmlcoinDB -> channel closed")
				err := q.db.Close()
				dbCloseChan <- err
				return
			} else {
				q.logger.Info("Got result!")
			}
			q.logger = q.logger.WithFields(logrus.Fields{
				"blockNum": pair.BlockNumber,
			})
			q.logger.Debug(" Received new pair of hashes")
			if q.logger.Level != logrus.DebugLevel {
				progBar.Add(1)
			}

			if pair.BlockNumber%PROGRESS_LEVEL_THRESHOLD == 0 {
				duration := time.Since(start)
				fmt.Println()
				q.logger.WithFields(logrus.Fields{
					"elapsedTime":     duration,
					"blocksProcessed": PROGRESS_LEVEL_THRESHOLD,
				}).Info("progress checkpoint")
				start = time.Now()
				progBar = getBar(PROGRESS_LEVEL_THRESHOLD)
			}
			_, err := q.insert(ctx, pair.BlockNumber, chainId, pair.EthHash, pair.HtmlcoinHash)
			if err != nil {
				q.logger.Error("error writing to db: ", err, " for block: ", pair.BlockNumber)
				q.errChan <- err
				return
			}
			q.records += 1
		}
	}()

}

func (q *HtmlcoinDB) GetRecords() int64 {
	return q.records
}

// creates a progress bar used to display progress when only 1 alien is left
func getBar(I int) *progressbar.ProgressBar {
	bar := progressbar.NewOptions(I,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetDescription("[cyan][reset]==>Processing 10K Htmlcoin blocks..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	return bar
}
