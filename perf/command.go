//
// DISCLAIMER
//
// Copyright 2016-2021 ArangoDB GmbH, Cologne, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright holder is ArangoDB GmbH, Cologne, Germany
//

package perf

import (
	"context"
	"crypto/tls"
	"math/rand"
	"sync"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/connection"
	"github.com/spf13/cobra"
	"golang.org/x/net/http2"
)

type Command struct {
	Servers []string

	User, Pass, Database, Collection string

	Threads, Bulk int

	HTTPS bool

	GenerateInput struct {
		Documents int
		Output    string
	}

	PerfInput struct {
		File         string
		Modification string
	}
}

func (c *Command) Init(cmd *cobra.Command) {
	f := cmd.PersistentFlags()

	f.StringSliceVar(&c.Servers, "endpoints", nil, "List of endpoints to connect")
	f.StringVar(&c.User, "user", "root", "Username")
	f.StringVar(&c.Pass, "password", "", "Password")
	f.StringVar(&c.Database, "database", "benchmark", "Database")
	f.StringVar(&c.Collection, "collection", "benchmark", "Collection")
	f.BoolVar(&c.HTTPS, "https", false, "Determine if https is enabled")

	f.IntVar(&c.Threads, "threads", 2, "Number of threads")
	f.IntVar(&c.Bulk, "bulk", 32, "Bulk size")

	genCmd := &cobra.Command{
		Use:  "generate",
		RunE: c.generate,
	}

	gf := genCmd.Flags()

	gf.IntVar(&c.GenerateInput.Documents, "count", 1024, "Number of documents")
	gf.StringVar(&c.GenerateInput.Output, "output", "./output", "Place for saved file")

	perfCmd := &cobra.Command{
		Use:  "perf",
		RunE: c.perf,
	}

	gf = perfCmd.Flags()

	gf.StringVar(&c.PerfInput.File, "file", "", "Path to the file with input")
	gf.StringVar(&c.PerfInput.Modification, "modification", "{}", "JSON document transformation")

	cmd.AddCommand(genCmd, perfCmd)
}

func (c *Command) client() arangodb.Client {
	auth := connection.NewBasicAuth(c.User, c.Pass)

	cfg := connection.Http2Configuration{
		Authentication: auth,
		Endpoint:       connection.NewEndpoints(c.shuffleEndpoint()...),
		ContentType:    connection.ApplicationJSON,
		Transport: &http2.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	if !c.HTTPS {
		cfg.Transport.AllowHTTP = true
		cfg.Transport.DialTLS = connection.NewHTTP2DialForEndpoint(connection.NewEndpoints(c.shuffleEndpoint()...))
	}

	conn := connection.NewHttp2Connection(cfg)

	return arangodb.NewClient(conn)
}

func (c *Command) databases(ctx context.Context) ([]arangodb.Database, error) {
	dbs := make([]arangodb.Database, c.Threads)

	for i := 0; i < c.Threads; i++ {
		if db, err := c.database(ctx); err != nil {
			return nil, err
		} else {
			dbs[i] = db
		}
	}

	return dbs, nil
}

func (c *Command) collections(ctx context.Context) ([]arangodb.Collection, error) {
	dbs := make([]arangodb.Collection, c.Threads)

	var wg sync.WaitGroup

	for i := 0; i < c.Threads; i++ {

		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			if db, err := c.collection(ctx); err != nil {
				println(err.Error())
				return
			} else {
				dbs[id] = db
			}
		}(i)
	}

	wg.Wait()

	return dbs, nil
}

func (c *Command) database(ctx context.Context) (arangodb.Database, error) {
	return c.client().Database(ctx, c.Database)
}

func (c *Command) collection(ctx context.Context) (arangodb.Collection, error) {
	d, err := c.client().Database(ctx, c.Database)

	if err != nil {
		return nil, err
	}

	return d.Collection(ctx, c.Collection)
}

func (c *Command) generateIterationChannel(iterations int) <-chan int {
	w := make(chan int, iterations)

	for i := 0; i < iterations; i++ {
		w <- i
	}

	close(w)

	return w
}

func (c *Command) runBulk(maxBulkSize int, iterations int, iteration func(thread int, ids []int) error) RequestResult {
	var wg sync.WaitGroup

	var res RequestResult

	ch := c.generateIterationChannel(iterations)
	results := make(Results, iterations)

	start := time.Now()

	for i := 0; i < c.Threads; i++ {
		wg.Add(1)

		go func(thread int) {
			defer wg.Done()

			bulk := make([]int, maxBulkSize)
			bulkSize := 0

			for id := range ch {
				bulk[bulkSize] = id
				bulkSize++
				if bulkSize >= maxBulkSize {

					now := time.Now()
					results[bulk[0]].Error = iteration(thread, bulk)
					results[bulk[0]].Time = time.Now().Sub(now)

					bulkSize = 0
				}
			}

			if bulkSize > 0 {
				now := time.Now()
				results[bulk[0]].Error = iteration(thread, bulk[0:bulkSize])
				results[bulk[0]].Time = time.Now().Sub(now)
			}
		}(i)
	}

	wg.Wait()

	t, err := results.Compact()

	res.Error = err
	res.Times = t
	res.Threads = c.Threads
	res.Bulk = c.Bulk
	res.Duration = time.Now().Sub(start)
	res.Items = iterations

	return res
}

func (c *Command) shuffleEndpoint() []string {
	var z []string

	z = append(z, c.Servers...)

	rand.Shuffle(len(z), func(i, j int) {
		z[i], z[j] = z[j], z[i]
	})

	return z
}
