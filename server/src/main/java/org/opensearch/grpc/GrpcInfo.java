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

package org.opensearch.grpc;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.service.ReportingService;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

// TODO: IDENTICAL TO - HTTP INFO

/**
 * Information about a grpc connection
 *
 * @opensearch.api
 */
@ExperimentalApi
public class GrpcInfo implements ReportingService.Info {
    private final BoundTransportAddress address;
    private final long maxContentLength;

    public GrpcInfo(StreamInput in) throws IOException {
        this(new BoundTransportAddress(in), in.readLong());
    }

    public GrpcInfo(BoundTransportAddress address, long maxContentLength) {
        this.address = address;
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
        out.writeLong(maxContentLength);
    }

    static final class Fields {
        static final String GRPC = "grpc";
        static final String BOUND_ADDRESS = "bound_address";
        static final String PUBLISH_ADDRESS = "publish_address";
        static final String MAX_CONTENT_LENGTH = "max_content_length";
        static final String MAX_CONTENT_LENGTH_IN_BYTES = "max_content_length_in_bytes";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.GRPC);
        builder.array(Fields.BOUND_ADDRESS, (Object[]) address.boundAddresses());
        TransportAddress publishAddress = address.publishAddress();
        String publishAddressString = publishAddress.toString();
        String hostString = publishAddress.address().getHostString();
        if (InetAddresses.isInetAddress(hostString) == false) {
            publishAddressString = hostString + '/' + publishAddress.toString();
        }
        builder.field(Fields.PUBLISH_ADDRESS, publishAddressString);
        builder.humanReadableField(Fields.MAX_CONTENT_LENGTH_IN_BYTES, Fields.MAX_CONTENT_LENGTH, maxContentLength());
        builder.endObject();
        return builder;
    }

    public BoundTransportAddress address() {
        return address;
    }

    public BoundTransportAddress getAddress() {
        return address();
    }

    public ByteSizeValue maxContentLength() {
        return new ByteSizeValue(maxContentLength);
    }

    public ByteSizeValue getMaxContentLength() {
        return maxContentLength();
    }
}
