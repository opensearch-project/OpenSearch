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

package org.opensearch.indices.template;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.io.IOException;
import java.util.Collections;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class IndexTemplateBlocksIT extends OpenSearchIntegTestCase {
    public void testIndexTemplatesWithBlocks() throws IOException {
        // creates a simple index template
        client().admin()
            .indices()
            .preparePutTemplate("template_blocks")
            .setPatterns(Collections.singletonList("te*"))
            .setOrder(0)
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "text")
                    .field("store", true)
                    .endObject()
                    .startObject("field2")
                    .field("type", "keyword")
                    .field("store", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .execute()
            .actionGet();

        try {
            setClusterReadOnly(true);

            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("template_blocks").execute().actionGet();
            assertThat(response.getIndexTemplates(), hasSize(1));

            assertBlocked(
                client().admin()
                    .indices()
                    .preparePutTemplate("template_blocks_2")
                    .setPatterns(Collections.singletonList("block*"))
                    .setOrder(0)
                    .addAlias(new Alias("alias_1"))
            );

            assertBlocked(client().admin().indices().prepareDeleteTemplate("template_blocks"));

        } finally {
            setClusterReadOnly(false);
        }
    }
}
