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
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/arangodb/go-example1/pkg/performance/parallel"

	"github.com/dchest/uniuri"
	"github.com/spf13/cobra"
)

func (c *Command) generate(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cs, err := c.collections(ctx)
	if err != nil {
		return err
	}

	rid := strings.ToLower(uniuri.NewLen(12))
	keys := make([]string, c.GenerateInput.Documents)
	documents := make([]map[string]string, c.GenerateInput.Documents)

	for i := 0; i < len(documents); i++ {
		key := fmt.Sprintf("doc-%s-%d", rid, i)
		keys[i] = key
		documents[i] = map[string]string{
			"_key": key,
		}
	}

	batches := parallel.NewBatches(len(documents), c.Batch)

	result := batches.Run(c.Threads, func(thread int, batch parallel.Batch) error {
		_, err := cs[thread].CreateDocuments(ctx, documents[batch.Start:batch.End])
		return err
	})

	println(result.String())

	if result.Error != nil {
		return result.Error
	}

	if err := ioutil.WriteFile(c.GenerateInput.Output, []byte(strings.Join(keys, "\n")), 0644); err != nil {
		return err
	}

	return nil
}
