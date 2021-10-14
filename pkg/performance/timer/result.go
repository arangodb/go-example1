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

package timer

import (
	"fmt"
	"strings"
	"time"
)

type Result struct {
	Times Times
	Error error

	Duration time.Duration
	Items    int
	Bulk     int
	Threads  int
}

func (r Result) String() string {
	var lines []string
	lines = append(lines, fmt.Sprintf("Operation of %d documents on %d threads with bulk size %d:", r.Items, r.Threads, r.Bulk))
	lines = append(lines, fmt.Sprintf("\tOperation Took: %s", r.Duration.String()))
	lines = append(lines, fmt.Sprintf("\tOperation Per Document Took: %s", (r.Duration/time.Duration(r.Items)).String()))
	lines = append(lines, fmt.Sprintf("\tDocuments per Second: %d/s", int(float64(r.Items)/(float64(r.Duration)/float64(time.Second)))))

	return strings.Join(lines, "\n")
}
