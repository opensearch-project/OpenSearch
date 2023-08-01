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

package org.opensearch.client.core;

import org.opensearch.client.AbstractResponseTestCase;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;

public final class GetSourceResponseTests extends AbstractResponseTestCase<GetSourceResponseTests.SourceOnlyResponse, GetSourceResponse> {

    static class SourceOnlyResponse implements ToXContentObject {

        private final BytesReference source;

        SourceOnlyResponse(BytesReference source) {
            this.source = source;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // this implementation copied from RestGetSourceAction.RestGetSourceResponseListener::buildResponse
            try (InputStream stream = source.streamInput()) {
                builder.rawValue(stream, MediaTypeRegistry.xContentType(source));
            }
            return builder;
        }
    }

    @Override
    protected SourceOnlyResponse createServerTestInstance(XContentType xContentType) {
        BytesReference source = new BytesArray("{\"field\":\"value\"}");
        return new SourceOnlyResponse(source);
    }

    @Override
    protected GetSourceResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetSourceResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(SourceOnlyResponse serverTestInstance, GetSourceResponse clientInstance) {
        assertThat(clientInstance.getSource(), equalTo(Collections.singletonMap("field", "value")));
    }
}
