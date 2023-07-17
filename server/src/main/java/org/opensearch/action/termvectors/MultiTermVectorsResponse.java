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

package org.opensearch.action.termvectors;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A multi get response.
 *
 * @opensearch.internal
 */
public class MultiTermVectorsResponse extends ActionResponse implements Iterable<MultiTermVectorsItemResponse>, ToXContentObject {

    /**
     * Represents a failure.
     *
     * @opensearch.internal
     */
    public static class Failure implements Writeable {
        private final String index;
        private final String id;
        private final Exception cause;

        public Failure(String index, String id, Exception cause) {
            this.index = index;
            this.id = id;
            this.cause = cause;
        }

        public Failure(StreamInput in) throws IOException {
            index = in.readString();
            if (in.getVersion().before(Version.V_2_0_0)) {
                // ignore removed type from pre-2.0.0 versions
                in.readOptionalString();
            }
            id = in.readString();
            cause = in.readException();
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The id of the action.
         */
        public String getId() {
            return id;
        }

        /**
         * The failure cause.
         */
        public Exception getCause() {
            return this.cause;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            if (out.getVersion().before(Version.V_2_0_0)) {
                // types no longer supported
                out.writeOptionalString(null);
            }
            out.writeString(id);
            out.writeException(cause);
        }
    }

    private final MultiTermVectorsItemResponse[] responses;

    public MultiTermVectorsResponse(MultiTermVectorsItemResponse[] responses) {
        this.responses = responses;
    }

    public MultiTermVectorsResponse(StreamInput in) throws IOException {
        super(in);
        responses = in.readArray(MultiTermVectorsItemResponse::new, MultiTermVectorsItemResponse[]::new);
    }

    public MultiTermVectorsItemResponse[] getResponses() {
        return this.responses;
    }

    @Override
    public Iterator<MultiTermVectorsItemResponse> iterator() {
        return Arrays.stream(responses).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(Fields.DOCS);
        for (MultiTermVectorsItemResponse response : responses) {
            if (response.isFailed()) {
                builder.startObject();
                Failure failure = response.getFailure();
                builder.field(Fields._INDEX, failure.getIndex());
                builder.field(Fields._ID, failure.getId());
                OpenSearchException.generateFailureXContent(builder, params, failure.getCause(), true);
                builder.endObject();
            } else {
                TermVectorsResponse getResponse = response.getResponse();
                getResponse.toXContent(builder, params);
            }
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String DOCS = "docs";
        static final String _INDEX = "_index";
        static final String _ID = "_id";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(responses);
    }
}
