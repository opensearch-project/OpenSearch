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

package org.opensearch.action.bulk;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class BulkProcessorClusterSettingsIT extends OpenSearchIntegTestCase {
    public void testBulkProcessorAutoCreateRestrictions() throws Exception {
        // See issue #8125
        Settings settings = Settings.builder().put("action.auto_create_index", false).build();

        internalCluster().startNode(settings);

        createIndex("willwork");
        client().admin().cluster().prepareHealth("willwork").setWaitForGreenStatus().execute().actionGet();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("willwork").setId("1").setSource("{\"foo\":1}", MediaTypeRegistry.JSON));
        bulkRequestBuilder.add(client().prepareIndex("wontwork").setId("2").setSource("{\"foo\":2}", MediaTypeRegistry.JSON));
        bulkRequestBuilder.add(client().prepareIndex("willwork").setId("3").setSource("{\"foo\":3}", MediaTypeRegistry.JSON));
        BulkResponse br = bulkRequestBuilder.get();
        BulkItemResponse[] responses = br.getItems();
        assertEquals(3, responses.length);
        assertFalse("Operation on existing index should succeed", responses[0].isFailed());
        assertTrue("Missing index should have been flagged", responses[1].isFailed());
        assertEquals("[wontwork] IndexNotFoundException[no such index [wontwork]]", responses[1].getFailureMessage());
        assertFalse("Operation on existing index should succeed", responses[2].isFailed());
    }
}
