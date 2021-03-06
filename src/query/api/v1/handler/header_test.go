// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package handler

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/query/block"

	"github.com/stretchr/testify/assert"
)

func TestAddWarningHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	meta := block.NewResultMetadata()
	AddWarningHeaders(recorder, meta)
	assert.Equal(t, 0, len(recorder.Header()))

	recorder = httptest.NewRecorder()
	meta.Exhaustive = false
	ex := LimitHeaderSeriesLimitApplied
	AddWarningHeaders(recorder, meta)
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(LimitHeader))

	recorder = httptest.NewRecorder()
	meta.AddWarning("foo", "bar")
	ex = fmt.Sprintf("%s,%s_%s", LimitHeaderSeriesLimitApplied, "foo", "bar")
	AddWarningHeaders(recorder, meta)
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(LimitHeader))

	recorder = httptest.NewRecorder()
	meta.Exhaustive = true
	ex = "foo_bar"
	AddWarningHeaders(recorder, meta)
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(LimitHeader))
}
