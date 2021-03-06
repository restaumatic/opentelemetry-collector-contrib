// Copyright 2019, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package translation

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	sfxpb "github.com/signalfx/com_signalfx_metrics_protobuf/model"
	"go.uber.org/zap"
)

// Action is the enum to capture actions to perform on metrics.
type Action string

const (
	// ActionRenameDimensionKeys renames dimension keys using Rule.Mapping.
	ActionRenameDimensionKeys Action = "rename_dimension_keys"

	// ActionRenameMetrics renames metrics using Rule.Mapping.
	ActionRenameMetrics Action = "rename_metrics"

	// ActionMultiplyInt scales integer metrics by multiplying their values using
	// Rule.ScaleFactorsInt key/values as metric_name/multiplying_factor
	ActionMultiplyInt Action = "multiply_int"

	// ActionDivideInt scales integer metric by dividing their values using
	// Rule.ScaleFactorsInt key/values as metric_name/divisor
	ActionDivideInt Action = "divide_int"

	// ActionMultiplyFloat scales integer metric by dividing their values using
	// Rule.ScaleFactorsFloat key/values as metric_name/multiplying_factor
	ActionMultiplyFloat Action = "multiply_float"

	// ActionConvertValues converts float metrics values to integer values using
	// Rule.TypesMapping key/values as metric_name/new_type.
	ActionConvertValues Action = "convert_values"

	// ActionCopyMetrics copies metrics using Rule.Mapping.
	// Rule.DimensionKey and Rule.DimensionValues can be used to filter datapoints that must be copied,
	// if these fields are set, only metics having a dimension with key == Rule.DimensionKey and
	// value in Rule.DimensionValues will be copied.
	ActionCopyMetrics Action = "copy_metrics"

	// ActionSplitMetric splits a metric with Rule.MetricName into multiple metrics
	// based on a dimension specified in Rule.DimensionKey.
	// Rule.Mapping represents "dimension value" -> "new metric name" for the translation.
	// For example, having the following translation rule:
	//   - action: split_metric
	// 	   metric_name: k8s.pod.network.io
	//     dimension_key: direction
	//     mapping:
	//       receive: pod_network_receive_bytes_total
	//       transmit: pod_network_transmit_bytes_total
	// The following translations will be performed:
	// k8s.pod.network.io{direction="receive"} -> pod_network_receive_bytes_total{}
	// k8s.pod.network.io{direction="transmit"} -> pod_network_transmit_bytes_total{}
	ActionSplitMetric Action = "split_metric"

	// ActionAggregateMetric aggregates metrics by dimensions provided in tr.Dimensions.
	// It takes datapoints with name tr.MetricName and aggregates them to a smaller set keeping the same name.
	// It drops all other dimensions other then those that match tr.Dimensions.
	// If a datapoint doesn't have a dimension matchin tr.Dimensions, the datapoint will be dropped.
	// tr.AggregationMethod is used to specify a method to aggregate the values.
	// For example, having the following translation rule:
	// - action: aggregate_metric
	//   metric_name: machine_cpu_cores
	//   aggregation_method: count
	//   dimensions:
	// 	 - host
	// The following translations will be performed:
	// Original datapoints:
	//   machine_cpu_cores{cpu="cpu1",host="host1"} 0.22
	//   machine_cpu_cores{cpu="cpu2",host="host1"} 0.11
	//   machine_cpu_cores{cpu="cpu1",host="host2"} 0.33
	// Transformed datapoints:
	//   machine_cpu_cores{host="host1"} 2
	//   machine_cpu_cores{host="host2"} 1
	ActionAggregateMetric Action = "aggregate_metric"

	// ActionCalculateNewMetric calculates a new metric based on two existing metrics.
	// It takes two operand metrics, an operator, and a metric name and produces a new metric with the given
	// metric name, but with the attributes of the first operand metric.
	// For example, for the following translation rule:
	// - action: calculate_new_metric
	//  metric_name: memory.utilization
	//  operand1_metric: memory.used
	//  operand2_metric: memory.total
	//  operator: /
	// the integer value of the 'memory.used' metric will be divided by the integer value of 'memory.total'. The
	// result will be a new float metric with the name 'memory.utilization' and the value of the quotient. The
	// new metric will also get any attributes of the 'memory.used' metric except for its value and metric name.
	// Currently only integer inputs are handled and only division is supported.
	ActionCalculateNewMetric Action = "calculate_new_metric"
)

type MetricOperator string

const (
	MetricOperatorDivision MetricOperator = "/"
)

// MetricValueType is the enum to capture valid metric value types that can be converted
type MetricValueType string

const (
	// MetricValueTypeInt represents integer metric value type
	MetricValueTypeInt MetricValueType = "int"
	// MetricValueTypeDouble represents double metric value type
	MetricValueTypeDouble MetricValueType = "double"
)

// AggregationMethod is the enum used to capture aggregation method
type AggregationMethod string

const (
	// AggregationMethodCount represents count aggregation method
	AggregationMethodCount AggregationMethod = "count"
	AggregationMethodSum   AggregationMethod = "sum"
)

type Rule struct {
	// Action specifies the translation action to be applied on metrics.
	// This is a required field.
	Action Action `mapstructure:"action"`

	// Mapping specifies key/value mapping that is used by rename_dimension_keys,
	// rename_metrics, copy_metrics, and split_metric actions.
	Mapping map[string]string `mapstructure:"mapping"`

	// ScaleFactorsInt is used by multiply_int and divide_int action to scale
	// integer metric values, key/value format: metric_name/scale_factor
	ScaleFactorsInt map[string]int64 `mapstructure:"scale_factors_int"`

	// ScaleFactorsInt is used by multiply_float action to scale
	// float metric values, key/value format: metric_name/scale_factor
	ScaleFactorsFloat map[string]float64 `mapstructure:"scale_factors_float"`

	// MetricName is used by "split_metric" translation rule to specify a name
	// of a metric that will be split.
	MetricName string `mapstructure:"metric_name"`
	// DimensionKey is used by "split_metric" translation rule action to specify dimension key
	// that will be used to translate the metric datapoints. Datapoints that don't have
	// the specified dimension key will not be translated.
	// DimensionKey is also used by "copy_metrics" for filterring.
	DimensionKey string `mapstructure:"dimension_key"`

	// DimensionValues is used by "copy_metrics" to filter out datapoints with dimensions values
	// not matching values set in this field
	DimensionValues map[string]bool `mapstructure:"dimension_values"`

	// TypesMapping is represents metric_name/metric_type key/value pairs,
	// used by ActionConvertValues.
	TypesMapping map[string]MetricValueType `mapstructure:"types_mapping"`

	// AggregationMethod specifies method used by "aggregate_metric" translation rule
	AggregationMethod AggregationMethod `mapstructure:"aggregation_method"`

	// Dimensions is used by "aggregate_metric" translation rule to specify dimension keys
	// that will be used to aggregate the metric across.
	// Datapoints that don't have all the dimensions will be dropped.
	Dimensions []string `mapstructure:"dimensions"`

	Operand1Metric string         `mapstructure:"operand1_metric"`
	Operand2Metric string         `mapstructure:"operand2_metric"`
	Operator       MetricOperator `mapstructure:"operator"`
}

type MetricTranslator struct {
	rules []Rule

	// Additional map to be used only for dimension renaming in metadata
	dimensionsMap map[string]string
}

func NewMetricTranslator(rules []Rule) (*MetricTranslator, error) {
	err := validateTranslationRules(rules)
	if err != nil {
		return nil, err
	}

	return &MetricTranslator{
		rules:         rules,
		dimensionsMap: createDimensionsMap(rules),
	}, nil
}

func validateTranslationRules(rules []Rule) error {
	var renameDimentionKeysFound bool
	for _, tr := range rules {
		switch tr.Action {
		case ActionRenameDimensionKeys:
			if tr.Mapping == nil {
				return fmt.Errorf("field \"mapping\" is required for %q translation rule", tr.Action)
			}
			if renameDimentionKeysFound {
				return fmt.Errorf("only one %q translation rule can be specified", tr.Action)
			}
			renameDimentionKeysFound = true
		case ActionRenameMetrics:
			if tr.Mapping == nil {
				return fmt.Errorf("field \"mapping\" is required for %q translation rule", tr.Action)
			}
		case ActionMultiplyInt:
			if tr.ScaleFactorsInt == nil {
				return fmt.Errorf("field \"scale_factors_int\" is required for %q translation rule", tr.Action)
			}
		case ActionDivideInt:
			if tr.ScaleFactorsInt == nil {
				return fmt.Errorf("field \"scale_factors_int\" is required for %q translation rule", tr.Action)
			}
			for k, v := range tr.ScaleFactorsInt {
				if v == 0 {
					return fmt.Errorf("\"scale_factors_int\" for %q translation rule has 0 value for %q metric", tr.Action, k)
				}
			}
		case ActionMultiplyFloat:
			if tr.ScaleFactorsFloat == nil {
				return fmt.Errorf("field \"scale_factors_float\" is required for %q translation rule", tr.Action)
			}
		case ActionCopyMetrics:
			if tr.Mapping == nil {
				return fmt.Errorf("field \"mapping\" is required for %q translation rule", tr.Action)
			}
			if tr.DimensionKey != "" && len(tr.DimensionValues) == 0 {
				return fmt.Errorf(
					"\"dimension_values_filer\" has to be provided if \"dimension_key\" is set for %q translation rule",
					tr.Action)
			}
		case ActionSplitMetric:
			if tr.MetricName == "" || tr.DimensionKey == "" || tr.Mapping == nil {
				return fmt.Errorf(
					"fields \"metric_name\", \"dimension_key\", and \"mapping\" are required for %q translation rule",
					tr.Action)
			}
		case ActionConvertValues:
			if tr.TypesMapping == nil {
				return fmt.Errorf("field \"types_mapping\" are required for %q translation rule", tr.Action)
			}
			for k, v := range tr.TypesMapping {
				if v != MetricValueTypeInt && v != MetricValueTypeDouble {
					return fmt.Errorf("invalid value type %q set for metric %q in \"types_mapping\"", v, k)
				}
			}
		case ActionAggregateMetric:
			if tr.MetricName == "" || tr.AggregationMethod == "" || len(tr.Dimensions) == 0 {
				return fmt.Errorf("fields \"metric_name\", \"dimensions\", and \"aggregation_method\" "+
					"are required for %q translation rule", tr.Action)
			}
			if tr.AggregationMethod != "count" && tr.AggregationMethod != "sum" {
				return fmt.Errorf("invalid \"aggregation_method\": %q provided for %q translation rule",
					tr.AggregationMethod, tr.Action)
			}
		case ActionCalculateNewMetric:
			if tr.MetricName == "" || tr.Operand1Metric == "" || tr.Operand2Metric == "" || tr.Operator == "" {
				return fmt.Errorf(`fields "metric_name", "operand1_metric", "operand2_metric", and "operator" are `+
					"required for %q translation rule", tr.Action)
			}
			if tr.Operator != MetricOperatorDivision {
				return fmt.Errorf("invalid operator %q for %q translation rule", tr.Operator, tr.Action)
			}

		default:
			return fmt.Errorf("unknown \"action\" value: %q", tr.Action)
		}
	}
	return nil
}

// createDimensionsMap creates an additional map for dimensions
// from ActionRenameDimensionKeys actions in rules.
func createDimensionsMap(rules []Rule) map[string]string {
	for _, tr := range rules {
		if tr.Action == ActionRenameDimensionKeys {
			return tr.Mapping
		}
	}

	return nil
}

// TranslateDataPoints transforms datapoints to a format compatible with signalfx backend
// sfxDataPoints represents one metric converted to signalfx protobuf datapoints
func (mp *MetricTranslator) TranslateDataPoints(logger *zap.Logger, sfxDataPoints []*sfxpb.DataPoint) []*sfxpb.DataPoint {
	processedDataPoints := sfxDataPoints

	for _, tr := range mp.rules {
		switch tr.Action {
		case ActionRenameDimensionKeys:
			for _, dp := range processedDataPoints {
				for _, d := range dp.Dimensions {
					if newKey, ok := tr.Mapping[d.Key]; ok {
						d.Key = newKey
					}
				}
			}
		case ActionRenameMetrics:
			for _, dp := range processedDataPoints {
				if newKey, ok := tr.Mapping[dp.Metric]; ok {
					dp.Metric = newKey
				}
			}
		case ActionMultiplyInt:
			for _, dp := range processedDataPoints {
				if multiplier, ok := tr.ScaleFactorsInt[dp.Metric]; ok {
					v := dp.GetValue().IntValue
					if v != nil {
						*v = *v * multiplier
					}
				}
			}
		case ActionDivideInt:
			for _, dp := range processedDataPoints {
				if divisor, ok := tr.ScaleFactorsInt[dp.Metric]; ok {
					v := dp.GetValue().IntValue
					if v != nil {
						*v = *v / divisor
					}
				}
			}
		case ActionMultiplyFloat:
			for _, dp := range processedDataPoints {
				if multiplier, ok := tr.ScaleFactorsFloat[dp.Metric]; ok {
					v := dp.GetValue().DoubleValue
					if v != nil {
						*v = *v * multiplier
					}
				}
			}
		case ActionCopyMetrics:
			for _, dp := range processedDataPoints {
				if newMetric, ok := tr.Mapping[dp.Metric]; ok {
					newDataPoint := copyMetric(tr, dp, newMetric)
					if newDataPoint != nil {
						processedDataPoints = append(processedDataPoints, newDataPoint)
					}
				}
			}
		case ActionSplitMetric:
			for _, dp := range processedDataPoints {
				if tr.MetricName == dp.Metric {
					splitMetric(dp, tr.DimensionKey, tr.Mapping)
				}
			}
		case ActionConvertValues:
			for _, dp := range processedDataPoints {
				if newType, ok := tr.TypesMapping[dp.Metric]; ok {
					convertMetricValue(logger, dp, newType)
				}
			}
		case ActionCalculateNewMetric:
			var operand1, operand2 *sfxpb.DataPoint
			for _, dp := range processedDataPoints {
				if dp.Metric == tr.Operand1Metric {
					operand1 = dp
				} else if dp.Metric == tr.Operand2Metric {
					operand2 = dp
				}
			}
			newPt := calculateNewMetric(logger, operand1, operand2, tr)
			if newPt == nil {
				continue
			}
			processedDataPoints = append(processedDataPoints, newPt)

		case ActionAggregateMetric:
			// NOTE: Based on the usage of TranslateDataPoints we can assume that the datapoints batch []*sfxpb.DataPoint
			// represents only one metric and all the datapoints can be aggregated together.
			var dpsToAggregate []*sfxpb.DataPoint
			var otherDps []*sfxpb.DataPoint
			for i, dp := range processedDataPoints {
				if dp.Metric == tr.MetricName {
					if dpsToAggregate == nil {
						dpsToAggregate = make([]*sfxpb.DataPoint, 0, len(processedDataPoints)-i)
					}
					dpsToAggregate = append(dpsToAggregate, dp)
				} else {
					if otherDps == nil {
						otherDps = make([]*sfxpb.DataPoint, 0, len(processedDataPoints)-i)
					}
					// This slice can contain additional datapoints from a different metric
					// for example copied in a translation step before
					otherDps = append(otherDps, dp)
				}
			}
			aggregatedDps := aggregateDatapoints(logger, dpsToAggregate, tr.Dimensions, tr.AggregationMethod)
			processedDataPoints = append(otherDps, aggregatedDps...)
		}
	}

	return processedDataPoints
}

func calculateNewMetric(
	logger *zap.Logger,
	operand1 *sfxpb.DataPoint,
	operand2 *sfxpb.DataPoint,
	tr Rule,
) *sfxpb.DataPoint {
	if operand1 == nil {
		logger.Warn(
			"calculate_new_metric: no matching datapoint found for operand1 to calculate new metric",
			zap.String("tr.Operand1Metric", tr.Operand1Metric),
			zap.String("tr.MetricName", tr.MetricName),
		)
		return nil
	}
	if operand1.Value.IntValue == nil {
		logger.Warn(
			"calculate_new_metric: operand1 has no IntValue",
			zap.String("tr.Operand1Metric", tr.Operand1Metric),
			zap.String("tr.MetricName", tr.MetricName),
		)
		return nil
	}

	if operand2 == nil {
		logger.Warn(
			"calculate_new_metric: no matching datapoint found for operand2 to calculate new metric",
			zap.String("tr.Operand2Metric", tr.Operand2Metric),
			zap.String("tr.MetricName", tr.MetricName),
		)
		return nil
	}
	if operand2.Value.IntValue == nil {
		logger.Warn(
			"calculate_new_metric: operand2 has no IntValue",
			zap.String("tr.Operand2Metric", tr.Operand2Metric),
			zap.String("tr.MetricName", tr.MetricName),
		)
		return nil
	}

	if tr.Operator == MetricOperatorDivision && *operand2.Value.IntValue == 0 {
		logger.Warn(
			"calculate_new_metric: attempt to divide by zero, skipping",
			zap.String("tr.Operand2Metric", tr.Operand2Metric),
			zap.String("tr.MetricName", tr.MetricName),
		)
		return nil
	}

	newPt := proto.Clone(operand1).(*sfxpb.DataPoint)
	newPt.Metric = tr.MetricName
	var newPtVal float64
	switch tr.Operator {
	// only supporting divide operator for now
	case MetricOperatorDivision:
		// only supporting int values for now
		newPtVal = float64(*operand1.Value.IntValue) / float64(*operand2.Value.IntValue)
	default:
		logger.Warn("calculate_new_metric: unsupported operator", zap.String("operator", string(tr.Operator)))
		return nil
	}
	newPt.Value = sfxpb.Datum{DoubleValue: &newPtVal}
	return newPt
}

func (mp *MetricTranslator) TranslateDimension(orig string) string {
	if translated, ok := mp.dimensionsMap[orig]; ok {
		return translated
	}
	return orig
}

// aggregateDatapoints aggregates datapoints assuming that they have
// the same Timestamp, MetricType, Metric and Source fields.
func aggregateDatapoints(
	logger *zap.Logger,
	dps []*sfxpb.DataPoint,
	dimensionsKeys []string,
	aggregation AggregationMethod,
) []*sfxpb.DataPoint {
	if len(dps) == 0 {
		return nil
	}

	// group datapoints by dimension values
	dimValuesToDps := make(map[string][]*sfxpb.DataPoint, len(dps))
	for i, dp := range dps {
		aggregationKey, err := getAggregationKey(dp.Dimensions, dimensionsKeys)
		if err != nil {
			logger.Debug("datapoint is dropped", zap.String("metric", dp.Metric), zap.Error(err))
			continue
		}
		if _, ok := dimValuesToDps[aggregationKey]; !ok {
			// set slice capacity to the possible maximum = len(dps)-i to avoid reallocations
			dimValuesToDps[aggregationKey] = make([]*sfxpb.DataPoint, 0, len(dps)-i)
		}
		dimValuesToDps[aggregationKey] = append(dimValuesToDps[aggregationKey], dp)
	}

	// Get aggregated results
	result := make([]*sfxpb.DataPoint, 0, len(dimValuesToDps))
	for _, dps := range dimValuesToDps {
		dp := proto.Clone(dps[0]).(*sfxpb.DataPoint)
		dp.Dimensions = filterDimensions(dp.Dimensions, dimensionsKeys)
		switch aggregation {
		case AggregationMethodCount:
			gauge := sfxpb.MetricType_GAUGE
			dp.MetricType = &gauge
			value := int64(len(dps))
			dp.Value = sfxpb.Datum{
				IntValue: &value,
			}
		case AggregationMethodSum:
			var intValue int64
			var floatValue float64
			value := sfxpb.Datum{}
			for _, dp := range dps {
				if dp.Value.IntValue != nil {
					intValue += *dp.Value.IntValue
					value.IntValue = &intValue
				}
				if dp.Value.DoubleValue != nil {
					floatValue += *dp.Value.DoubleValue
					value.DoubleValue = &floatValue
				}
			}
			dp.Value = value
		}
		result = append(result, dp)
	}

	return result
}

// getAggregationKey composes an aggregation key based on provided dimensions.
// If all the dimensions found, the function returns an aggregationkey.
// If any dimension os not found the function returns an error.
func getAggregationKey(dimensions []*sfxpb.Dimension, dimensionsKeys []string) (string, error) {
	const aggregationKeyDelimiter = "//"
	var aggregationKey string
	for _, dk := range dimensionsKeys {
		var dimensionFound bool
		for _, d := range dimensions {
			if d.Key == dk {
				// compose an aggregation key with "//" as delimiter
				aggregationKey += d.Value + aggregationKeyDelimiter
				dimensionFound = true
				continue
			}
		}
		if !dimensionFound {
			return "", fmt.Errorf("dimension to aggregate by is not found: %q", dk)
		}
	}
	return aggregationKey, nil
}

// filterDimensions returns list of dimension filtered by dimensionsKeys
func filterDimensions(dimensions []*sfxpb.Dimension, dimensionsKeys []string) []*sfxpb.Dimension {
	if len(dimensions) == 0 || len(dimensionsKeys) == 0 {
		return nil
	}
	result := make([]*sfxpb.Dimension, 0, len(dimensionsKeys))
	for _, dk := range dimensionsKeys {
		for _, d := range dimensions {
			if d.Key == dk {
				result = append(result, d)
				continue
			}
		}
	}
	return result
}

// splitMetric renames a metric with "dimension key" == dimensionKey to mapping["dimension value"],
// datapoint not changed if not dimension found equal to dimensionKey:mapping->key.
func splitMetric(dp *sfxpb.DataPoint, dimensionKey string, mapping map[string]string) {
	if len(dp.Dimensions) == 0 {
		return
	}

	dimensions := make([]*sfxpb.Dimension, 0, len(dp.Dimensions)-1)
	var match bool
	for i, d := range dp.Dimensions {
		if dimensionKey == d.Key {
			if newName, ok := mapping[d.Value]; ok {
				// The dimension value matches the mapping, proceeding
				dp.Metric = newName
				match = true
				continue
			}
			// The dimension value doesn't match the mapping, keep the datapoint as is
			return
		}

		// No dimension key found for the specified dimensionKey, keep the datapoint as is
		if i == len(dp.Dimensions)-1 && !match {
			return
		}

		dimensions = append(dimensions, d)
	}

	dp.Dimensions = dimensions
}

func convertMetricValue(logger *zap.Logger, dp *sfxpb.DataPoint, newType MetricValueType) {
	switch newType {
	case MetricValueTypeInt:
		val := dp.GetValue().DoubleValue
		if val == nil {
			logger.Debug("only datapoint of \"double\" type can be converted to int",
				zap.String("metric", dp.Metric))
			return
		}
		var intVal = int64(*val)
		dp.Value = sfxpb.Datum{IntValue: &intVal}
	case MetricValueTypeDouble:
		val := dp.GetValue().IntValue
		if val == nil {
			logger.Debug("only datapoint of \"int\" type can be converted to double",
				zap.String("metric", dp.Metric))
			return
		}
		var floatVal = float64(*val)
		dp.Value = sfxpb.Datum{DoubleValue: &floatVal}
	}
}

func copyMetric(tr Rule, dp *sfxpb.DataPoint, newMetricName string) *sfxpb.DataPoint {
	if tr.DimensionKey != "" {
		var match bool
		for _, d := range dp.Dimensions {
			if d.Key == tr.DimensionKey {
				match = tr.DimensionValues[d.Value]
				break
			}
		}
		if !match {
			return nil
		}
	}

	newDataPoint := proto.Clone(dp).(*sfxpb.DataPoint)
	newDataPoint.Metric = newMetricName
	return newDataPoint
}
