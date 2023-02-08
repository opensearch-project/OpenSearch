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

import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Map;

public class PutMappingRequestTests extends AbstractXContentTestCase<PutMappingRequest> {

    @Override
    protected PutMappingRequest createTestInstance() {
        PutMappingRequest request = new PutMappingRequest();
        if (frequently()) {
            try {
                XContentBuilder builder = RandomCreateIndexGenerator.randomMapping();
                request.source(builder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return request;
    }

    @Override
    protected PutMappingRequest doParseInstance(XContentParser parser) throws IOException {
        PutMappingRequest request = new PutMappingRequest();
        Map<String, Object> map = parser.map();
        if (map.isEmpty() == false) {
            request.source(map);
        }
        return request;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(PutMappingRequest expected, PutMappingRequest actual) {
        if (actual.source() != null) {
            try (
                XContentParser expectedJson = createParser(expected.mediaType().xContent(), expected.source());
                XContentParser actualJson = createParser(actual.mediaType().xContent(), actual.source())
            ) {
                assertEquals(expectedJson.mapOrdered(), actualJson.mapOrdered());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // if the original `source` is null, the parsed source should be so too
            assertNull(expected.source());
        }
    }
}
