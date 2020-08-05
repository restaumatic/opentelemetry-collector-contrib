module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogagentexporter

go 1.14

require (
	github.com/DataDog/opencensus-go-exporter-datadog v0.0.0-20200406135749-5c268882acf0 // indirect
	github.com/census-instrumentation/opencensus-proto v0.3.0
	github.com/golang/protobuf v1.4.2
	github.com/open-telemetry/opentelemetry-proto v0.4.0
	github.com/stretchr/testify v1.6.1
	go.opencensus.io v0.22.4
	go.opentelemetry.io/collector v0.5.1-0.20200728200651-9cbf43e372f0
	go.opentelemetry.io/otel v0.6.0
	go.uber.org/zap v1.15.0
	golang.org/x/tools v0.0.0-20200804234916-fec4f28ebb08 // indirect
	gopkg.in/DataDog/dd-trace-go.v1 v1.22.0
)
