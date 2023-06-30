/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.http;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.transport.ProtobufBoundTransportAddress;
import org.opensearch.common.transport.ProtobufTransportAddress;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;

/**
 * Information about an http connection
*
* @opensearch.internal
*/
public class ProtobufHttpInfo implements ProtobufReportingService.ProtobufInfo {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ProtobufHttpInfo.class);

    /** Deprecated property, just here for deprecation logging in 7.x. */
    private static final boolean CNAME_IN_PUBLISH_HOST = System.getProperty("opensearch.http.cname_in_publish_address") != null;

    private final ProtobufBoundTransportAddress address;
    private final long maxContentLength;

    public ProtobufHttpInfo(CodedInputStream in) throws IOException {
        this(new ProtobufBoundTransportAddress(in), in.readInt64());
    }

    public ProtobufHttpInfo(ProtobufBoundTransportAddress address, long maxContentLength) {
        this.address = address;
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        address.writeTo(out);
        out.writeInt64NoTag(maxContentLength);
    }

    public ProtobufBoundTransportAddress address() {
        return address;
    }

    public ProtobufBoundTransportAddress getAddress() {
        return address();
    }

    public ByteSizeValue maxContentLength() {
        return new ByteSizeValue(maxContentLength);
    }

    public ByteSizeValue getMaxContentLength() {
        return maxContentLength();
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String BOUND_ADDRESS = "bound_address";
        static final String PUBLISH_ADDRESS = "publish_address";
        static final String MAX_CONTENT_LENGTH = "max_content_length";
        static final String MAX_CONTENT_LENGTH_IN_BYTES = "max_content_length_in_bytes";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.array(Fields.BOUND_ADDRESS, (Object[]) address.boundAddresses());
        ProtobufTransportAddress publishAddress = address.publishAddress();
        String publishAddressString = publishAddress.toString();
        String hostString = publishAddress.address().getHostString();
        if (CNAME_IN_PUBLISH_HOST) {
            deprecationLogger.deprecate(
                "cname_in_publish_address",
                "opensearch.http.cname_in_publish_address system property is deprecated and no longer affects http.publish_address "
                    + "formatting. Remove this property to get rid of this deprecation warning."
            );
        }
        if (InetAddresses.isInetAddress(hostString) == false) {
            publishAddressString = hostString + '/' + publishAddress.toString();
        }
        builder.field(Fields.PUBLISH_ADDRESS, publishAddressString);
        builder.humanReadableField(Fields.MAX_CONTENT_LENGTH_IN_BYTES, Fields.MAX_CONTENT_LENGTH, maxContentLength());
        builder.endObject();
        return builder;
    }
}
