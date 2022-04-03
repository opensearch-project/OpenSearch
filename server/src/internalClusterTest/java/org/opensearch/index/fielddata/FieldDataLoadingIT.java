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

package org.opensearch.index.fielddata;

import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

public class FieldDataLoadingIT extends OpenSearchIntegTestCase {

    public void testEagerGlobalOrdinalsFieldDataLoading() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "text")
                    .field("fielddata", true)
                    .field("eager_global_ordinals", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "name").get();
        client().admin().indices().prepareRefresh("test").get();

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(0L));
    }

}
