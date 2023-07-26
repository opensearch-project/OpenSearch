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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Identifies an object that supplies a filter for the content of a {@link RestRequest}. This interface should be implemented by a
 * {@link org.opensearch.rest.RestHandler} that expects there will be sensitive content in the body of the request such as a password
 *
 * @opensearch.api
 */
public interface RestRequestFilter {

    /**
     * Wraps the RestRequest and returns a version that provides the filtered content
     */
    default RestRequest getFilteredRequest(RestRequest restRequest) throws IOException {
        Set<String> fields = getFilteredFields();
        if (restRequest.hasContent() && fields.isEmpty() == false) {
            return new RestRequest(restRequest) {

                private BytesReference filteredBytes = null;

                @Override
                public boolean hasContent() {
                    return true;
                }

                @Override
                public BytesReference content() {
                    if (filteredBytes == null) {
                        BytesReference content = restRequest.content();
                        Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(content, true);
                        Map<String, Object> transformedSource = XContentMapValues.filter(
                            result.v2(),
                            null,
                            fields.toArray(Strings.EMPTY_ARRAY)
                        );
                        try {
                            XContentBuilder xContentBuilder = XContentBuilder.builder(result.v1().xContent()).map(transformedSource);
                            filteredBytes = BytesReference.bytes(xContentBuilder);
                        } catch (IOException e) {
                            throw new OpenSearchException("failed to parse request", e);
                        }
                    }
                    return filteredBytes;
                }
            };
        } else {
            return restRequest;
        }
    }

    /**
     * The list of fields that should be filtered. This can be a dot separated pattern to match sub objects and also supports wildcards
     */
    Set<String> getFilteredFields();
}
