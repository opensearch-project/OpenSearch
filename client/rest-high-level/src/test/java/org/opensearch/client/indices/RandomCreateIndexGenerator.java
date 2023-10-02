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

package org.opensearch.client.indices;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.index.RandomCreateIndexGenerator.randomAlias;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;

public class RandomCreateIndexGenerator {

    /**
     * Returns a random {@link CreateIndexRequest}.
     *
     * Randomizes the index name, the aliases, mappings and settings associated with the
     * index. When present, the mappings make no mention of types.
     */
    public static CreateIndexRequest randomCreateIndexRequest() {
        try {
            // Create a random server request, and copy its contents into the HLRC request.
            // Because client requests only accept typeless mappings, we must swap out the
            // mapping definition for one that does not contain types.
            org.opensearch.action.admin.indices.create.CreateIndexRequest serverRequest = org.opensearch.index.RandomCreateIndexGenerator
                .randomCreateIndexRequest();
            return new CreateIndexRequest(serverRequest.index()).settings(serverRequest.settings())
                .aliases(serverRequest.aliases())
                .mapping(randomMapping());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a random mapping, with no mention of types.
     */
    public static XContentBuilder randomMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        org.opensearch.index.RandomCreateIndexGenerator.randomMappingFields(builder, true);
        builder.endObject();
        return builder;
    }

    /**
     * Sets random aliases to the provided {@link CreateIndexRequest}
     */
    public static void randomAliases(CreateIndexRequest request) {
        int aliasesNo = randomIntBetween(0, 2);
        for (int i = 0; i < aliasesNo; i++) {
            request.alias(randomAlias());
        }
    }
}
