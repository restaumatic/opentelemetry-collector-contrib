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
	"encoding/binary"
	"fmt"
	"net/http"
	"strconv"

	otlptrace "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"
	"go.opentelemetry.io/collector/consumer/pdata"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
)

const (
	keySamplingPriority = "_sampling_priority_v1"
	keySpanName         = "span.name"
)

func convertSpan(serviceName string, s pdata.Span, resource pdata.Resource) (*ddSpan, error) {
	span := &ddSpan{
		TraceID:  binary.BigEndian.Uint64(s.TraceID()[8:]), // Note: only the last 64 bits are used
		SpanID:   binary.BigEndian.Uint64(s.SpanID()),
		Name:     s.Name(),
		Resource: s.Name(),
		Service:  serviceName,
		Start:    int64(s.StartTime()),
		Duration: int64(s.EndTime() - s.StartTime()),
		Metrics:  map[string]float64{},
		Meta:     map[string]string{},
        Type:     "custom",
	}

    if len(s.ParentSpanID()) > 0 {
		span.ParentID = binary.BigEndian.Uint64(s.ParentSpanID())
    }

	code, ok := statusCodes[otlptrace.Status_StatusCode(s.Status().Code())]
	if !ok {
		code = codeDetails{
			message: "ERR_CODE_" + strconv.FormatInt(int64(s.Status().Code()), 10),
			status:  http.StatusInternalServerError,
		}
	}

	switch s.Kind() {
	case pdata.SpanKindCLIENT:
		span.Type = "client"
		if code.status/100 == 4 {
			span.Error = 1
		}
	case pdata.SpanKindSERVER:
		span.Type = "server"
		fallthrough
	default:
		if code.status/100 == 5 {
			span.Error = 1
		}
	}

	if span.Error == 1 {
		span.Meta[ext.ErrorType] = code.message
		if msg := s.Status().Message(); msg != "" {
			span.Meta[ext.ErrorMsg] = msg
		}
	}

	s.Attributes().ForEach(func(key string, val pdata.AttributeValue) {
		setTag(span, key, val)
	})
	return span, nil
}

func setTag(s *ddSpan, key string, v pdata.AttributeValue) {
	switch v.Type() {
	case pdata.AttributeValueSTRING:
		setStringTag(s, key, v.StringVal())
	case pdata.AttributeValueBOOL:
		setStringTag(s, key, strconv.FormatBool(v.BoolVal()))
	case pdata.AttributeValueDOUBLE:
		setMetric(s, key, v.DoubleVal())
	case pdata.AttributeValueINT:
		setMetric(s, key, float64(v.IntVal()))
	default:
		setStringTag(s, key, fmt.Sprintf("%v (Unknown OpenTelemetry attribute value type %q)", v, v.Type()))
	}
}

func setStringTag(s *ddSpan, key, v string) {
	switch key {
	case ext.ServiceName:
		s.Service = v
	case ext.ResourceName:
		s.Resource = v
	case ext.SpanType:
		s.Type = v
	case ext.AnalyticsEvent:
		if v != "false" {
			setMetric(s, ext.EventSampleRate, 1)
		} else {
			setMetric(s, ext.EventSampleRate, 0)
		}
	case keySpanName:
		s.Name = v
	default:
		s.Meta[key] = v
	}
}

func setMetric(s *ddSpan, key string, v float64) {
	switch key {
	case ext.SamplingPriority:
		s.Metrics[keySamplingPriority] = v
	default:
		s.Metrics[key] = v
	}
}

// ddSpan represents the Datadog span definition.
type ddSpan struct {
	SpanID   uint64             `json:"span_id"`
	TraceID  uint64             `json:"trace_id"`
	ParentID uint64             `json:"parent_id,omitempty"`
	Name     string             `json:"name"`
	Service  string             `json:"service"`
	Resource string             `json:"resource"`
	Type     string             `json:"type"`
	Start    int64              `json:"start"`
	Duration int64              `json:"duration"`
	Meta     map[string]string  `json:"meta,omitempty"`
	Metrics  map[string]float64 `json:"metrics,omitempty"`
	Error    int32              `json:"error"`
}

// statusCodes maps pdata.StatusCode to their message and http status code. See:
// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto.
var statusCodes = map[otlptrace.Status_StatusCode]codeDetails{
	otlptrace.Status_Ok:                 {message: "OK", status: http.StatusOK},
	otlptrace.Status_Cancelled:          {message: "CANCELLED", status: 499},
	otlptrace.Status_UnknownError:       {message: "UNKNOWN", status: http.StatusInternalServerError},
	otlptrace.Status_InvalidArgument:    {message: "INVALID_ARGUMENT", status: http.StatusBadRequest},
	otlptrace.Status_DeadlineExceeded:   {message: "DEADLINE_EXCEEDED", status: http.StatusGatewayTimeout},
	otlptrace.Status_NotFound:           {message: "NOT_FOUND", status: http.StatusNotFound},
	otlptrace.Status_AlreadyExists:      {message: "ALREADY_EXISTS", status: http.StatusConflict},
	otlptrace.Status_PermissionDenied:   {message: "PERMISSION_DENIED", status: http.StatusForbidden},
	otlptrace.Status_ResourceExhausted:  {message: "RESOURCE_EXHAUSTED", status: http.StatusTooManyRequests},
	otlptrace.Status_FailedPrecondition: {message: "FAILED_PRECONDITION", status: http.StatusBadRequest},
	otlptrace.Status_Aborted:            {message: "ABORTED", status: http.StatusConflict},
	otlptrace.Status_OutOfRange:         {message: "OUT_OF_RANGE", status: http.StatusBadRequest},
	otlptrace.Status_Unimplemented:      {message: "UNIMPLEMENTED", status: http.StatusNotImplemented},
	otlptrace.Status_InternalError:      {message: "INTERNAL", status: http.StatusInternalServerError},
	otlptrace.Status_Unavailable:        {message: "UNAVAILABLE", status: http.StatusServiceUnavailable},
	otlptrace.Status_DataLoss:           {message: "DATA_LOSS", status: http.StatusNotImplemented},
	otlptrace.Status_Unauthenticated:    {message: "UNAUTHENTICATED", status: http.StatusUnauthorized},
}

// codeDetails specifies information about a trace status code.
type codeDetails struct {
	message string // status message
	status  int    // corresponding HTTP status code
}
