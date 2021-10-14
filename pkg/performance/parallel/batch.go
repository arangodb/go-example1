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

type Batches struct {
	Batches    []Batch
	Iterations int
	BatchSize  int
}

type Batch struct {
	Start, End, ID int
}

func (b Batches) AsChannel() <-chan Batch {
	c := make(chan Batch, len(b.Batches))

	for _, z := range b.Batches {
		c <- z
	}

	close(c)

	return c
}

func NewBatches(size, batch int) Batches {
	var b []Batch

	var i = 0
	var id = 0
	for i < size {
		var z Batch
		z.ID = id
		z.Start = i
		if i+batch > size {
			i = size
		} else {
			i += batch
		}
		z.End = i

		id++

		b = append(b, z)
	}

	return Batches{
		Batches:    b,
		Iterations: size,
		BatchSize:  size,
	}
}
