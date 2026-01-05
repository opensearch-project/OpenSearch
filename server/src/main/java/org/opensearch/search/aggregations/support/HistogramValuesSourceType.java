/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.opensearch.script.AggregationScript;
import org.opensearch.search.DocValueFormat;

import java.util.function.LongSupplier;

public class HistogramValuesSourceType implements ValuesSourceType {
    public static final HistogramValuesSourceType HISTOGRAM = new HistogramValuesSourceType();

    private HistogramValuesSourceType() {
        super();
    }

    @Override
    public ValuesSource getEmpty() {
        return null;
    }

    @Override
    public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
        return null;
    }

    @Override
    public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
        return null;
    }

    @Override
    public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
        return null;
    }

    @Override
    public String typeName() {
        return "";
    }
}
