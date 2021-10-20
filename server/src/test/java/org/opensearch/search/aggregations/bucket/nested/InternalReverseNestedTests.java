/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket.nested;

import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.search.aggregations.bucket.ParsedSingleBucketAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalReverseNestedTests extends InternalSingleBucketAggregationTestCase<InternalReverseNested> {
    @Override
    protected InternalReverseNested createTestInstance(
        String name,
        long docCount,
        InternalAggregations aggregations,
        Map<String, Object> metadata
    ) {
        return new InternalReverseNested(name, docCount, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalReverseNested reduced, List<InternalReverseNested> inputs) {
        // Nothing extra to assert
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedReverseNested.class;
    }

    @Override
    protected void assertFromXContent(InternalReverseNested aggregation, ParsedAggregation parsedAggregation) throws IOException {
        super.assertFromXContent(aggregation, parsedAggregation);
        assertTrue(parsedAggregation instanceof ReverseNested);
    }
}
