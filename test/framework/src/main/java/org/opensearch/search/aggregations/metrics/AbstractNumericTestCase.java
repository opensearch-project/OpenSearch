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

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public abstract class AbstractNumericTestCase extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    protected static long minValue, maxValue, minValues, maxValues;

    public AbstractNumericTestCase(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        List<IndexRequestBuilder> builders = new ArrayList<>();

        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) { // TODO randomize the size and the params in here?
            builders.add(
                client().prepareIndex("idx")
                    .setId(String.valueOf(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field("value", i + 1)
                            .startArray("values")
                            .value(i + 2)
                            .value(i + 3)
                            .endArray()
                            .endObject()
                    )
            );
        }
        minValue = 1;
        minValues = 2;
        maxValue = numDocs;
        maxValues = numDocs + 2;
        indexRandom(true, builders);

        // creating an index to test the empty buckets functionality. The way it works is by indexing
        // two docs {value: 0} and {value : 2}, then building a histogram agg with interval 1 and with empty
        // buckets computed.. the empty bucket is the one associated with key "1". then each test will have
        // to check that this bucket exists with the appropriate sub aggregations.
        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").execute().actionGet();
        builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId(String.valueOf(i))
                    .setSource(jsonBuilder().startObject().field("value", i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testEmptyAggregation() throws Exception {}

    public void testUnmapped() throws Exception {}

    public void testSingleValuedField() throws Exception {}

    public void testSingleValuedFieldGetProperty() throws Exception {}

    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {}

    public void testSingleValuedFieldWithValueScript() throws Exception {}

    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {}

    public void testMultiValuedField() throws Exception {}

    public void testMultiValuedFieldWithValueScript() throws Exception {}

    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {}

    public void testScriptSingleValued() throws Exception {}

    public void testScriptSingleValuedWithParams() throws Exception {}

    public void testScriptMultiValued() throws Exception {}

    public void testScriptMultiValuedWithParams() throws Exception {}

    public void testOrderByEmptyAggregation() throws Exception {}
}
