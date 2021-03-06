// Copyright (c) 2018 Uber Technologies, Inc.
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

package native

import (
	"context"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// CompleteTagsURL is the url for searching tags.
	CompleteTagsURL = handler.RoutePrefixV1 + "/search"

	// CompleteTagsHTTPMethod is the HTTP method used with this resource.
	CompleteTagsHTTPMethod = http.MethodGet
)

// CompleteTagsHandler represents a handler for search tags endpoint.
type CompleteTagsHandler struct {
	storage             storage.Storage
	fetchOptionsBuilder handler.FetchOptionsBuilder
	instrumentOpts      instrument.Options
}

// NewCompleteTagsHandler returns a new instance of handler.
func NewCompleteTagsHandler(
	storage storage.Storage,
	fetchOptionsBuilder handler.FetchOptionsBuilder,
	instrumentOpts instrument.Options,
) http.Handler {
	return &CompleteTagsHandler{
		storage:             storage,
		fetchOptionsBuilder: fetchOptionsBuilder,
		instrumentOpts:      instrumentOpts,
	}
}

func (h *CompleteTagsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)
	w.Header().Set("Content-Type", "application/json")

	query, rErr := prometheus.ParseTagCompletionParamsToQuery(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	opts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	result, err := h.storage.CompleteTags(ctx, query, opts)
	if err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	handler.AddWarningHeaders(w, result.Metadata)
	if err = prometheus.RenderTagCompletionResultsJSON(w, result); err != nil {
		logger.Error("unable to render results", zap.Error(err))
	}
}
