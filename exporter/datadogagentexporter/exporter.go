// Copyright 2020 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datadogagentexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

func NewTraceExporter(config *Config, logger *zap.Logger) (component.TraceExporter, error) {
	client := &http.Client{}
	return exporterhelper.NewTraceExporter(
		config,
		func(ctx context.Context, td pdata.Traces) (totalDroppedSpans int, err error) {
			totalDroppedSpans = 0

			traceIDToIndex := make(map[uint64]int)
			var traces [][]*ddSpan

			for i := 0; i < td.ResourceSpans().Len(); i++ {
				rspans := td.ResourceSpans().At(i)
				if rspans.IsNil() {
					continue
				}

				resource := rspans.Resource()
				for j := 0; j < rspans.InstrumentationLibrarySpans().Len(); j++ {
					ispans := rspans.InstrumentationLibrarySpans().At(j)
					if ispans.IsNil() {
						continue
					}

					spans := ispans.Spans()
					for k := 0; k < spans.Len(); k++ {
						span := spans.At(k)
						if span.IsNil() {
							continue
						}

						ddspan, localErr := convertSpan(config.ServiceName, span, resource)
						if localErr != nil {
							totalDroppedSpans++
							continue
						}

						traceIndex, ok := traceIDToIndex[ddspan.TraceID]
						if !ok {
							traceIndex = len(traces)
							traceIDToIndex[ddspan.TraceID] = traceIndex
							traces = append(traces, []*ddSpan{})
						}
						traces[traceIndex] = append(traces[traceIndex], ddspan)
					}
				}
			}
			sendTraces(config, client, traces)
			return totalDroppedSpans, err
		},
		exporterhelper.WithShutdown(func(context.Context) error {
			return logger.Sync()
		}),
	)
}

func sendTraces(config *Config, client *http.Client, traces [][]*ddSpan) (droppedSpans int, err error) {
	numSpans := 0
	for _, trace := range traces {
		numSpans += len(trace)
	}

	body := new(bytes.Buffer)
	err = json.NewEncoder(body).Encode(traces)
	if err != nil {
		return numSpans, consumererror.Permanent(err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v0.4/traces", config.AgentURL), body)
	if err != nil {
		return numSpans, consumererror.Permanent(err)
	}

	req.Header.Set("Content-type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return numSpans, err
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return 0, nil
}
