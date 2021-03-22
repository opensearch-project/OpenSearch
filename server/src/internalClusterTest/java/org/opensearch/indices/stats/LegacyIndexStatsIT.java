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

package org.opensearch.indices.stats;

import org.opensearch.LegacyESVersion;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LegacyIndexStatsIT extends OpenSearchIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testFieldDataFieldsParam() {
        assertAcked(client()
                .admin()
                .indices()
                .prepareCreate("test1")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), LegacyESVersion.V_6_0_0))
                .addMapping("_doc", "bar", "type=text,fielddata=true", "baz", "type=text,fielddata=true")
                .get());

        ensureGreen();

        client().prepareIndex("test1", "_doc", Integer.toString(1)).setSource("{\"bar\":\"bar\",\"baz\":\"baz\"}", XContentType.JSON).get();
        client().prepareIndex("test1", "_doc", Integer.toString(2)).setSource("{\"bar\":\"bar\",\"baz\":\"baz\"}", XContentType.JSON).get();
        refresh();

        client().prepareSearch("_all").addSort("bar", SortOrder.ASC).addSort("baz", SortOrder.ASC).execute().actionGet();

        final IndicesStatsRequestBuilder builder = client().admin().indices().prepareStats();

        {
            final IndicesStatsResponse stats = builder.execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields(), is(nullValue()));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("bar").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(false));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("bar", "baz").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("baz"), greaterThan(0L));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("*").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("baz"), greaterThan(0L));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("*r").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(false));
        }

    }

}
