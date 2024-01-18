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

import java.util.Comparator;


/** Aggregation function, doc values column pair */
public class AggregationFunctionColumnPair implements Comparable<AggregationFunctionColumnPair> {
    public static final String DELIMITER = "__";
    public static final String STAR = "*";
    public static final AggregationFunctionColumnPair COUNT_STAR =
        new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, STAR);

    private final AggregationFunctionType _functionType;
    private final String _column;

    public AggregationFunctionColumnPair(AggregationFunctionType functionType, String column) {
        _functionType = functionType;
        if (functionType == AggregationFunctionType.COUNT) {
            _column = STAR;
        } else {
            _column = column;
        }
    }

    public AggregationFunctionType getFunctionType() {
        return _functionType;
    }

    public String getColumn() {
        return _column;
    }

    public String toColumnName() {
        return toColumnName(_functionType, _column);
    }

    public static String toColumnName(AggregationFunctionType functionType, String column) {
        return functionType.getName() + DELIMITER + column;
    }

    public static AggregationFunctionColumnPair fromColumnName(String columnName) {
        String[] parts = columnName.split(DELIMITER, 2);
        return fromFunctionAndColumnName(parts[0], parts[1]);
    }

    private static AggregationFunctionColumnPair fromFunctionAndColumnName(String functionName, String columnName) {
        AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
        if (functionType == AggregationFunctionType.COUNT) {
            return COUNT_STAR;
        } else {
            return new AggregationFunctionColumnPair(functionType, columnName);
        }
    }

    @Override
    public int hashCode() {
        return 31 * _functionType.hashCode() + _column.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof AggregationFunctionColumnPair) {
            AggregationFunctionColumnPair anotherPair = (AggregationFunctionColumnPair) obj;
            return _functionType == anotherPair._functionType && _column.equals(anotherPair._column);
        }
        return false;
    }

    @Override
    public String toString() {
        return toColumnName();
    }

    @Override
    public int compareTo(AggregationFunctionColumnPair other) {
        return Comparator.comparing((AggregationFunctionColumnPair o) -> o._column)
            .thenComparing((AggregationFunctionColumnPair o) -> o._functionType).compare(this, other);
    }
}
