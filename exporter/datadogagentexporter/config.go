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

import "go.opentelemetry.io/collector/config/configmodels"

// Config defines configuration options for the DataDog exporter.
type Config struct {
	configmodels.ExporterSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	// AgentURL is the URL of the DataDog Agent. Default: `http://localhost:8126`.
	AgentURL string `mapstructure:"agent_url"`
	// ServiceName is the default service name for your spans (will be used if `service.name` span attribute is not provided)
	ServiceName string `mapstructure:"service_name"`
}
