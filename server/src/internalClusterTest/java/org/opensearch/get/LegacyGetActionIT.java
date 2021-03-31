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

package org.opensearch.get;

import org.opensearch.LegacyESVersion;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.get.GetActionIT.indexOrAlias;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LegacyGetActionIT extends OpenSearchIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testGetFieldsMetadataWithRouting() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("_doc", "field1", "type=keyword,store=true")
                .addAlias(new Alias("alias"))
                .setSettings(
                        Settings.builder()
                                .put("index.refresh_interval", -1)
                                // multi-types in 6.0.0
                                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), LegacyESVersion.V_6_0_0)));

        try (XContentBuilder source = jsonBuilder().startObject().field("field1", "value").endObject()) {
            client()
                    .prepareIndex("test", "_doc", "1")
                    .setRouting("1")
                    .setSource(source)
                    .get();
        }

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "_doc", "1")
                    .setRouting("1")
                    .setStoredFields("field1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }

        flush();

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "_doc", "1")
                    .setStoredFields("field1")
                    .setRouting("1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }
    }

}
