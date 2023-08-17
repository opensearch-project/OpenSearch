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

package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.MergePolicyConfig;
import org.opensearch.indices.IndexClosedException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import java.util.Collection;

import static org.hamcrest.Matchers.is;

public class IndicesSegmentsRequestTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    @Before
    public void setupIndex() {
        Settings settings = Settings.builder()
            // don't allow any merges so that the num docs is the expected segments
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .build();
        createIndex("test", settings);

        int numDocs = scaledRandomIntBetween(100, 1000);
        for (int j = 0; j < numDocs; ++j) {
            String id = Integer.toString(j);
            client().prepareIndex("test").setId(id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush("test").get();
        client().admin().indices().prepareRefresh().get();
    }

    /**
     * with the default IndicesOptions inherited from BroadcastOperationRequest this will raise an exception
     */
    public void testRequestOnClosedIndex() {
        client().admin().indices().prepareClose("test").get();
        try {
            client().admin().indices().prepareSegments("test").get();
            fail("Expected IndexClosedException");
        } catch (IndexClosedException e) {
            assertThat(e.getMessage(), is("closed"));
        }
    }

    /**
     * setting the "ignoreUnavailable" option prevents IndexClosedException
     */
    public void testRequestOnClosedIndexIgnoreUnavailable() {
        client().admin().indices().prepareClose("test").get();
        IndicesOptions defaultOptions = new IndicesSegmentsRequest().indicesOptions();
        IndicesOptions testOptions = IndicesOptions.fromOptions(true, true, true, false, defaultOptions);
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments("test").setIndicesOptions(testOptions).get();
        assertEquals(0, rsp.getIndices().size());
    }

    /**
     * by default IndicesOptions setting IndicesSegmentsRequest should not throw exception when no index present
     */
    public void testAllowNoIndex() {
        client().admin().indices().prepareDelete("test").get();
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments().get();
        assertEquals(0, rsp.getIndices().size());
    }
}
