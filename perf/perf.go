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
	"os"

	"github.com/spf13/cobra"
)

func (c *Command) perf(cmd *cobra.Command, args []string) error {
	f, err := os.Open(c.PerfInput.File)
	if err != nil {
		return err
	}

	defer f.Close()

	transformation := map[string]interface{}{}

	if err := json.Unmarshal([]byte(c.PerfInput.Modification), &transformation); err != nil {
		return err
	}

	println("Starting")

	scanner := bufio.NewScanner(f)

	scanner.Split(bufio.ScanLines)

	var lines []string

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	ctx := context.Background()

	cs, err := c.collections(ctx)
	if err != nil {
		return err
	}

	documents := make([]map[string]interface{}, len(lines))

	println("Reading documents")

	result := c.runBulk(c.Bulk, len(lines), func(thread int, ids []int) error {
		client := cs[thread]

		keys := make([]string, len(ids))

		for i := 0; i < len(ids); i++ {
			keys[i] = lines[ids[i]]
		}

		reader, err := client.ReadDocuments(ctx, keys)
		if err != nil {
			return err
		}

		for i := 0; i < len(ids); i++ {
			obj := map[string]interface{}{}

			_, err := reader.Read(&obj)
			if err != nil {
				return err
			}

			documents[ids[i]] = obj
		}

		return nil
	})

	println(result.String())

	if result.Error != nil {
		return result.Error
	}

	println("Transform documents")

	for i := 0; i < len(documents); i++ {
		for k, v := range transformation {
			documents[i][k] = v
		}
	}

	println("Transform done")

	println("Updating documents")

	result = c.runBulk(c.Bulk, len(lines), func(thread int, ids []int) error {
		client := cs[thread]

		subDocs := make([]map[string]interface{}, len(ids))

		for i := 0; i < len(ids); i++ {
			subDocs[i] = documents[ids[i]]
		}

		reader, err := client.UpdateDocuments(ctx, subDocs)
		if err != nil {
			return err
		}

		for i := 0; i < len(ids); i++ {
			obj := map[string]interface{}{}

			_, err := reader.Read()
			if err != nil {
				return err
			}

			documents[ids[i]] = obj
		}

		return nil
	})

	println(result.String())

	if result.Error != nil {
		return result.Error
	}

	return nil
}
