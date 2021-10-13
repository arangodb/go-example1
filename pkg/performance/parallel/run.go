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

package parallel

import (
	"sync"
	"time"

	"github.com/arangodb/go-example1/pkg/performance/timer"
)

type Iterator func(thread int, batch Batch) error

func (b Batches) Run(threads int, iteration Iterator) timer.Result {
	var wg sync.WaitGroup

	ch := b.AsChannel()

	results := make(timer.Requests, len(b.Batches))

	start := time.Now()

	for i := 0; i < threads; i++ {
		wg.Add(1)

		go func(thread int) {
			defer wg.Done()

			for batch := range ch {
				now := time.Now()
				results[batch.ID].Error = iteration(thread, batch)
				results[batch.ID].Time = time.Since(now)
			}
		}(i)
	}

	wg.Wait()

	t, err := results.Compact()

	var res timer.Result
	res.Error = err
	res.Times = t
	res.Threads = threads
	res.Bulk = b.BatchSize
	res.Duration = time.Since(start)
	res.Items = b.Iterations

	return res
}
