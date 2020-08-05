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
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

func TestBasicSpan(t *testing.T) {
	testTraceExporter(t, constructTraces(constructExampleSpan()))
}

func TestOverrideSpanName(t *testing.T) {
    span := constructExampleSpan()
    span.Attributes().InsertString("span.name", "span_name_from_attribute")
	testTraceExporter(t, constructTraces(span))
}

func TestParentSpanID(t *testing.T) {
    span := constructExampleSpan()
	span.SetParentSpanID([]byte{101, 102, 103, 104, 105, 106, 107, 108})
	testTraceExporter(t, constructTraces(span))
}

func constructExampleSpan() *pdata.Span {
	span := pdata.NewSpan()
	span.InitEmpty()
	span.SetTraceID([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("span_name")
	span.SetKind(pdata.SpanKindINTERNAL)
	span.SetStartTime(pdata.TimestampUnixNano(1588334156464409000))
	span.SetEndTime(pdata.TimestampUnixNano(1588334156470454639))

	status := pdata.NewSpanStatus()
	status.InitEmpty()
	status.SetCode(0)
	status.SetMessage("OK")
	status.CopyTo(span.Status())

	return &span
}

func testingServer(callback func(data []byte)) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {

		defer req.Body.Close()
		b, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		callback(b)
		rw.Write([]byte(`OK`))
	}))
}

func testTraceExporter(t *testing.T, traces pdata.Traces) {
	testName := t.Name()

	responseLock := sync.Mutex{}

	response := []byte{}
	server := testingServer(func(data []byte) {
		responseLock.Lock()
		response = append(response, data...)
		responseLock.Unlock()
	})

	defer server.Close()
	cfg := Config{
		AgentURL:    server.URL,
		ServiceName: "test_service",
	}

	logger := zap.NewNop()
	factory := Factory{}
	exporter, err := factory.CreateTraceExporter(logger, &cfg)
	require.NoError(t, err)

	ctx := context.Background()
	err = exporter.ConsumeTraces(ctx, traces)
	require.NoError(t, err)
	exporter.Shutdown(ctx)

	responseLock.Lock()
	defer responseLock.Unlock()

	var formattedResponse bytes.Buffer
	err = json.Indent(&formattedResponse, response, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	assertSnapshot(t, formattedResponse.Bytes(), testName)
}

var update = flag.Bool("update", false, "update golden file")

// getGoldenFile returns the golden file content. If the `update` is specified, it updates the
// file with the current output and returns it.
// Based on <https://blog.gojekengineering.com/the-untold-story-of-golang-testing-29832bfe0e19>
func assertSnapshot(t *testing.T, actual []byte, testName string) {
	golden := filepath.Join("testdata", testName+".out")
	if *update {
		if err := ioutil.WriteFile(golden, actual, 0644); err != nil {
			t.Fatalf("Error writing golden file for filename=%s: %s", golden, err)
		}
	}
	expected, err := ioutil.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected golden file '%s' to match output:\n%s", t.Name(), string(actual))
	}
}

func constructTraces(span *pdata.Span) pdata.Traces {
	resource := constructResource()

	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(1)
	rspans := traces.ResourceSpans().At(0)
	resource.CopyTo(rspans.Resource())
	rspans.InstrumentationLibrarySpans().Resize(1)
	ispans := rspans.InstrumentationLibrarySpans().At(0)
	ispans.Spans().Resize(1)
	span.CopyTo(ispans.Spans().At(0))
	return traces
}

func constructResource() pdata.Resource {
	resource := pdata.NewResource()
	resource.InitEmpty()
	attrs := pdata.NewAttributeMap()
	attrs.CopyTo(resource.Attributes())
	return resource
}

func constructSpanAttributes(attributes map[string]interface{}) pdata.AttributeMap {
	attrs := pdata.NewAttributeMap()
	for key, value := range attributes {
		if cast, ok := value.(int); ok {
			attrs.InsertInt(key, int64(cast))
		} else if cast, ok := value.(int64); ok {
			attrs.InsertInt(key, cast)
		} else {
			attrs.InsertString(key, fmt.Sprintf("%v", value))
		}
	}
	return attrs
}
