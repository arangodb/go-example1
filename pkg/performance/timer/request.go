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

import "time"

type Requests []Request

func (r Requests) Compact() (Times, error) {
	rs := make(Times, len(r))

	for i := 0; i < len(r); i++ {
		if r[i].Error != nil {
			return nil, r[i].Error
		}

		rs[i] = r[i].Time
	}

	return rs, nil
}

type Request struct {
	Error error
	Time  time.Duration
}
