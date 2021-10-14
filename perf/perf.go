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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/arangodb/go-driver/v2/connection"
	"github.com/arangodb/go-example1/pkg/performance/parallel"
	"github.com/pkg/errors"

	"github.com/spf13/cobra"
)

func (c *Command) perf(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	conns := c.connections()

	transformation := map[string]interface{}{}

	if err := json.Unmarshal([]byte(c.PerfInput.Modification), &transformation); err != nil {
		return err
	}

	return c.updateDocuments(ctx, conns, transformation)
}

func (c *Command) updateDocuments(ctx context.Context, conns []connection.Connection, transform map[string]interface{}) error {
	docs, err := c.transformDocuments(ctx, conns, transform)
	if err != nil {
		return err
	}
	println("Updating documents with PATCH")

	batches := parallel.NewBatches(len(docs), c.Batch)

	result := batches.Run(c.Threads, func(thread int, batch parallel.Batch) error {
		client := conns[thread]

		req, err := client.NewRequest(http.MethodPatch, fmt.Sprintf("/_db/%s/_api/document/%s", c.Database, c.Collection))
		if err != nil {
			return err
		}

		if err := connection.WithBody(docs[batch.Start:batch.End])(req); err != nil {
			return err
		}
		if err := connection.WithQuery("silent", "true")(req); err != nil {
			return err
		}

		resp, data, err := client.Stream(ctx, req)
		if err != nil {
			return err
		}

		if resp.Code() != 200 && resp.Code() != 201 && resp.Code() != 202 {
			return errors.Errorf("Unknown response %d", resp.Code())
		}

		return data.Close()
	})

	println(result.String())

	if result.Error != nil {
		return result.Error
	}

	println("Updating documents with PUT")

	result = batches.Run(c.Threads, func(thread int, batch parallel.Batch) error {
		client := conns[thread]

		req, err := client.NewRequest(http.MethodPut, fmt.Sprintf("/_db/%s/_api/document/%s", c.Database, c.Collection))
		if err != nil {
			return err
		}

		if err := connection.WithBody(docs[batch.Start:batch.End])(req); err != nil {
			return err
		}
		if err := connection.WithQuery("silent", "true")(req); err != nil {
			return err
		}

		resp, data, err := client.Stream(ctx, req)
		if err != nil {
			return err
		}

		if resp.Code() != 200 && resp.Code() != 201 && resp.Code() != 202 {
			return errors.Errorf("Unknown response %d", resp.Code())
		}

		return data.Close()
	})

	println(result.String())

	if result.Error != nil {
		return result.Error
	}

	return nil
}

func (c *Command) transformDocuments(ctx context.Context, conns []connection.Connection, transform map[string]interface{}) ([]map[string]interface{}, error) {
	docs, err := c.readDocuments(ctx, conns)
	if err != nil {
		return nil, err
	}

	println("Transform documents")

	for i := 0; i < len(docs); i++ {
		for k, v := range transform {
			docs[i][k] = v
		}
	}

	println("Transform done")
	return docs, nil
}

func (c *Command) readDocuments(ctx context.Context, conns []connection.Connection) ([]map[string]interface{}, error) {
	keys, err := c.readDocumentKeys()
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, len(keys))

	println("Reading documents")

	batches := parallel.NewBatches(len(keys), c.Batch)

	results := batches.Run(c.Threads, func(thread int, batch parallel.Batch) error {
		client := conns[thread]

		req, err := client.NewRequest(http.MethodPut, fmt.Sprintf("/_db/%s/_api/document/%s", c.Database, c.Collection))
		if err != nil {
			return err
		}

		if err := connection.WithBody(keys[batch.Start:batch.End])(req); err != nil {
			return err
		}
		if err := connection.WithQuery("onlyget", "true")(req); err != nil {
			return err
		}

		resp, data, err := client.Stream(ctx, req)
		if err != nil {
			println(err.Error())
			return err
		}

		if resp.Code() != 200 && resp.Code() != 201 {
			return errors.Errorf("Unknown response %d", resp.Code())
		}

		var returnedData []map[string]interface{}
		deserializer := json.NewDecoder(data)
		err = deserializer.Decode(&returnedData)
		if err != nil {
			return err
		}

		for i := batch.Start; i < batch.End; i++ {
			result[i] = returnedData[i-batch.Start]

		}

		return data.Close()
	})

	println(results.String())

	if results.Error != nil {
		return nil, results.Error
	}

	return result, nil
}

func (c *Command) readDocumentKeys() ([]string, error) {
	f, err := os.Open(c.PerfInput.File)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(f)

	scanner.Split(bufio.ScanLines)

	var lines []string

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := f.Close(); err != nil {
		return nil, err
	}

	return lines, nil
}
