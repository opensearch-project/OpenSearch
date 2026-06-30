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

package org.opensearch.http;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for HTTP connections
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class HttpStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOpen;

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new HttpStats object.
     * @param builder The builder instance containing all the values.
     */
    private HttpStats(Builder builder) {
        this.serverOpen = builder.serverOpen;
        this.totalOpen = builder.totalOpen;
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public HttpStats(long serverOpen, long totalOpened) {
        this.serverOpen = serverOpen;
        this.totalOpen = totalOpened;
    }

    public HttpStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOpen = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOpen);
    }

    public long getServerOpen() {
        return this.serverOpen;
    }

    public long getTotalOpen() {
        return this.totalOpen;
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String CURRENT_OPEN = "current_open";
        static final String TOTAL_OPENED = "total_opened";
    }

    /**
     * Builder for the {@link HttpStats} class.
     * Provides a fluent API for constructing a HttpStats object.
     */
    public static class Builder {
        private long serverOpen = 0;
        private long totalOpen = 0;

        public Builder() {}

        public Builder serverOpen(long serverOpen) {
            this.serverOpen = serverOpen;
            return this;
        }

        public Builder totalOpen(long totalOpen) {
            this.totalOpen = totalOpen;
            return this;
        }

        /**
         * Creates a {@link HttpStats} object from the builder's current state.
         * @return A new HttpStats instance.
         */
        public HttpStats build() {
            return new HttpStats(this);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.field(Fields.CURRENT_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OPENED, totalOpen);
        builder.endObject();
        return builder;
    }
}
