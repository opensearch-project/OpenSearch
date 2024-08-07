/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype;

import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Numbers;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Numeric converters used during aggregations of metric values
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeNumericTypeConverters {

    public static double halfFloatPointToDouble(Long value) {
        return HalfFloatPoint.sortableShortToHalfFloat((short) value.longValue());
    }

    public static double floatPointToDouble(Long value) {
        return NumericUtils.sortableIntToFloat((int) value.longValue());
    }

    public static double longToDouble(Long value) {
        return (double) value;
    }

    public static double intToDouble(Long value) {
        return (double) value;
    }

    public static double shortToDouble(Long value) {
        return (double) value;
    }

    public static Double sortableLongtoDouble(Long value) {
        return NumericUtils.sortableLongToDouble(value);
    }

    public static double unsignedlongToDouble(Long value) {
        return Numbers.unsignedLongToDouble(value);
    }

    public static double bytesToDouble(Long value) {
        byte[] bytes = new byte[8];
        NumericUtils.longToSortableBytes(value, bytes, 0);
        return NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(bytes, 0));
    }

}
