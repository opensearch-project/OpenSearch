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

import org.opensearch.client.AbstractResponseTestCase;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GetMappingsResponseTests extends AbstractResponseTestCase<
    org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse,
    GetMappingsResponse> {

    @Override
    protected org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse createServerTestInstance(XContentType xContentType) {
        ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
        int numberOfIndexes = randomIntBetween(1, 5);
        for (int i = 0; i < numberOfIndexes; i++) {
            mappings.put("index-" + randomAlphaOfLength(5), randomMappingMetadata());
        }
        return new org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse(mappings.build());
    }

    @Override
    protected GetMappingsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetMappingsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse serverTestInstance,
        GetMappingsResponse clientInstance
    ) {
        assertMapEquals(serverTestInstance.getMappings(), clientInstance.mappings());
    }

    public static MappingMetadata randomMappingMetadata() {
        Map<String, Object> mappings = new HashMap<>();
        if (frequently()) { // rarely have no fields
            mappings.put("field1", randomFieldMapping());
            if (randomBoolean()) {
                mappings.put("field2", randomFieldMapping());
            }
        }
        return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mappings);
    }

    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomFrom("text", "keyword"));
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        }
        return mappings;
    }
}
