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

/**
 * A value aggregator that pre-aggregates on the input values for a specific type of aggregation.
 */
public interface ValueAggregator {

    /** Returns the type of the aggregation. */
    AggregationFunctionType getAggregationType();

    /** Returns the data type of the aggregated value. */
    DataType getAggregatedValueType();

    /** Returns the initial aggregated value. */
    Long getInitialAggregatedValue(Long rawValue);

    /**
     * Applies a raw value to the current aggregated value.
     *
     * <p>NOTE: if value is mutable, will directly modify the value.
     */
    Long applyRawValue(Long value, Long rawValue);

    /**
     * Applies an aggregated value to the current aggregated value.
     *
     * <p>NOTE: if value is mutable, will directly modify the value.
     */
    Long applyAggregatedValue(Long value, Long aggregatedValue);

    /** Clones an aggregated value. */
    Long cloneAggregatedValue(Long value);

    /** Returns the maximum size in bytes of the aggregated values seen so far. */
    int getMaxAggregatedValueByteSize();

    /** Serializes an aggregated value into a byte array. */
    byte[] serializeAggregatedValue(Long value);

    /** De-serializes an aggregated value from a byte array. */
    Long deserializeAggregatedValue(byte[] bytes);
}
