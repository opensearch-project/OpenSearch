/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.composite;

import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A functional interface which encapsulates the parsing function to be called for the aggregation which is
 * also registered as CompositeAggregation.
 */
@FunctionalInterface
public interface CompositeAggregationParsingFunction {
    CompositeValuesSourceBuilder<?> parse(final String name, final XContentParser parser) throws IOException;
}
