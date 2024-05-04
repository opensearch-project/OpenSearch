/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.queryshape.aggregations;

import org.opensearch.plugin.insights.rules.model.queryshape.misc.AggregationShape;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TermsAggregationShape extends AggregationShape {
    String fieldName;
    List<? extends AggregationShape> subAggregations;

    @Override
    public int hashCode() {
        if (subAggregations != null) {
            Collections.sort(subAggregations);
        }
        return Objects.hash(fieldName, subAggregations);
    }
}
