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

package org.opensearch.broadcast;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.transport.client.Requests.indexRequest;
import static org.hamcrest.Matchers.equalTo;

public class BroadcastActionsIT extends OpenSearchIntegTestCase {

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    public void testBroadcastOperations() throws IOException {
        assertAcked(prepareCreate("test", 1));

        NumShards numShards = getNumShards("test");

        logger.info("Running Cluster Health");
        client().index(indexRequest("test").id("1").source(source("1", "test"))).actionGet();
        flush();
        client().index(indexRequest("test").id("2").source(source("2", "test"))).actionGet();
        refresh();

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            SearchResponse countResponse = client().prepareSearch("test").setSize(0).setQuery(matchAllQuery()).get();
            assertThat(countResponse.getHits().getTotalHits().value(), equalTo(2L));
            assertThat(countResponse.getTotalShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }
    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
