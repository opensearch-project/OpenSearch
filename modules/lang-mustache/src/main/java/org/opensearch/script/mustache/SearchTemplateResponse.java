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

package org.opensearch.script.mustache;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SearchTemplateResponse extends ActionResponse implements StatusToXContentObject {
    public static ParseField TEMPLATE_OUTPUT_FIELD = new ParseField("template_output");

    /** Contains the source of the rendered template **/
    private BytesReference source;

    /** Contains the search response, if any **/
    private SearchResponse response;

    SearchTemplateResponse() {}

    SearchTemplateResponse(StreamInput in) throws IOException {
        super(in);
        source = in.readOptionalBytesReference();
        response = in.readOptionalWriteable(SearchResponse::new);
    }

    public BytesReference getSource() {
        return source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }

    public SearchResponse getResponse() {
        return response;
    }

    public void setResponse(SearchResponse searchResponse) {
        this.response = searchResponse;
    }

    public boolean hasResponse() {
        return response != null;
    }

    @Override
    public String toString() {
        return "SearchTemplateResponse [source=" + source + ", response=" + response + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBytesReference(source);
        out.writeOptionalWriteable(response);
    }

    public static SearchTemplateResponse fromXContent(XContentParser parser) throws IOException {
        SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
        Map<String, Object> contentAsMap = parser.map();

        if (contentAsMap.containsKey(TEMPLATE_OUTPUT_FIELD.getPreferredName())) {
            Object source = contentAsMap.get(TEMPLATE_OUTPUT_FIELD.getPreferredName());
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON).value(source);
            searchTemplateResponse.setSource(BytesReference.bytes(builder));
        } else {
            MediaType contentType = parser.contentType();
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(contentType).map(contentAsMap);
            XContentParser searchResponseParser = contentType.xContent()
                .createParser(parser.getXContentRegistry(), parser.getDeprecationHandler(), BytesReference.bytes(builder).streamInput());

            SearchResponse searchResponse = SearchResponse.fromXContent(searchResponseParser);
            searchTemplateResponse.setResponse(searchResponse);
        }
        return searchTemplateResponse;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasResponse()) {
            builder.startObject();
            response.innerToXContent(builder, params);
            builder.field(MultiSearchTemplateResponse.Fields.STATUS, response.status().getStatus());
            builder.endObject();
        } else {
            builder.startObject();
            // we can assume the template is always json as we convert it before compiling it
            try (InputStream stream = source.streamInput()) {
                builder.rawField(TEMPLATE_OUTPUT_FIELD.getPreferredName(), stream, MediaTypeRegistry.JSON);
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public RestStatus status() {
        if (hasResponse()) {
            return response.status();
        } else {
            return RestStatus.OK;
        }
    }
}
