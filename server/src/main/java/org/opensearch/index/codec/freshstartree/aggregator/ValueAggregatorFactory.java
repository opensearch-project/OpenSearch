/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.aggregator;

/** Value aggregator factory for a given aggregation type */
public class ValueAggregatorFactory {
    private ValueAggregatorFactory() {
    }

    /**
     * Returns a new instance of value aggregator for the given aggregation type.
     *
     * @param aggregationType Aggregation type
     * @return Value aggregator
     */
    public static ValueAggregator getValueAggregator(AggregationFunctionType aggregationType) {
        switch (aggregationType) {
            case COUNT:
                return new CountValueAggregator();
            case SUM:
                return new SumValueAggregator();
            //      case AVG:
            //        return new AvgValueAggregator();
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }

    /**
     * Returns the data type of the aggregated value for the given aggregation type.
     *
     * @param aggregationType Aggregation type
     * @return Data type of the aggregated value
     */
    public static DataType getAggregatedValueType(AggregationFunctionType aggregationType) {
        switch (aggregationType) {
            case COUNT:
                return CountValueAggregator.AGGREGATED_VALUE_TYPE;
            case SUM:
                return SumValueAggregator.AGGREGATED_VALUE_TYPE;
            //      case AVG:
            //        return AvgValueAggregator.AGGREGATED_VALUE_TYPE;
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }
}
