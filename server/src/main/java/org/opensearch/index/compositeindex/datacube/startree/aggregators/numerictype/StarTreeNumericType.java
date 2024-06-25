/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype;

import org.opensearch.index.fielddata.IndexNumericFieldData;

import java.util.function.Function;

/**
 * Enum to map Star Tree Numeric Types to Lucene's Numeric Type
 * @opensearch.experimental
 */
public enum StarTreeNumericType {
    HALF_FLOAT(IndexNumericFieldData.NumericType.HALF_FLOAT, StarTreeNumericTypeConverters::halfFloatPointToDouble),
    FLOAT(IndexNumericFieldData.NumericType.FLOAT, StarTreeNumericTypeConverters::floatPointToDouble),
    LONG(IndexNumericFieldData.NumericType.LONG, StarTreeNumericTypeConverters::longToDouble),
    DOUBLE(IndexNumericFieldData.NumericType.DOUBLE, StarTreeNumericTypeConverters::sortableLongtoDouble);

    final IndexNumericFieldData.NumericType numericType;
    final Function<Long, Double> converter;

    StarTreeNumericType(IndexNumericFieldData.NumericType numericType, Function<Long, Double> converter) {
        this.numericType = numericType;
        this.converter = converter;
    }

    public double getDoubleValue(long rawValue) {
        return this.converter.apply(rawValue);
    }

    public static StarTreeNumericType fromNumericType(IndexNumericFieldData.NumericType numericType) {
        switch (numericType) {
            case HALF_FLOAT:
                return StarTreeNumericType.HALF_FLOAT;
            case FLOAT:
                return StarTreeNumericType.FLOAT;
            case LONG:
                return StarTreeNumericType.LONG;
            case DOUBLE:
                return StarTreeNumericType.DOUBLE;
            default:
                throw new UnsupportedOperationException("Unknown numeric type [" + numericType + "]");
        }
    }
}
