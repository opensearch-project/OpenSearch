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

package org.opensearch.common.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * A transport address used for IP socket address (wraps {@link java.net.InetSocketAddress}).
*
* @opensearch.internal
*/
public final class ProtobufTransportAddress implements ProtobufWriteable, ToXContentFragment {

    /**
     * A <a href="https://en.wikipedia.org/wiki/0.0.0.0">non-routeable v4 meta transport address</a> that can be used for
    * testing or in scenarios where targets should be marked as non-applicable from a transport perspective.
    */
    public static final InetAddress META_ADDRESS;

    static {
        try {
            META_ADDRESS = InetAddress.getByName("0.0.0.0");
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    private final InetSocketAddress address;

    public ProtobufTransportAddress(InetAddress address, int port) {
        this(new InetSocketAddress(address, port));
    }

    public ProtobufTransportAddress(InetSocketAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("InetSocketAddress must not be null");
        }
        if (address.getAddress() == null) {
            throw new IllegalArgumentException("Address must be resolved but wasn't - InetSocketAddress#getAddress() returned null");
        }
        this.address = address;
    }

    /**
     * Read from a stream.
     */
    public ProtobufTransportAddress(CodedInputStream in) throws IOException {
        final int len = in.readRawByte();
        final byte[] a = new byte[len]; // 4 bytes (IPv4) or 16 bytes (IPv6)
        in.readRawBytes(len);
        String host = in.readString(); // the host string was serialized so we can ignore the passed in version
        final InetAddress inetAddress = InetAddress.getByAddress(host, a);
        int port = in.readInt32();
        this.address = new InetSocketAddress(inetAddress, port);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        byte[] bytes = address.getAddress().getAddress();  // 4 bytes (IPv4) or 16 bytes (IPv6)
        out.write((byte) bytes.length); // 1 byte
        out.write(bytes, 0, bytes.length);
        out.writeStringNoTag(address.getHostString());
        // don't serialize scope ids over the network!!!!
        // these only make sense with respect to the local machine, and will only formulate
        // the address incorrectly remotely.
        out.writeInt32NoTag(address.getPort());
    }

    /**
     * Returns a string representation of the enclosed {@link InetSocketAddress}
    * @see NetworkAddress#format(InetAddress)
    */
    public String getAddress() {
        return NetworkAddress.format(address.getAddress());
    }

    /**
     * Returns the addresses port
    */
    public int getPort() {
        return address.getPort();
    }

    /**
     * Returns the enclosed {@link InetSocketAddress}
    */
    public InetSocketAddress address() {
        return this.address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProtobufTransportAddress address1 = (ProtobufTransportAddress) o;
        return address.equals(address1.address);
    }

    @Override
    public int hashCode() {
        return address != null ? address.hashCode() : 0;
    }

    @Override
    public String toString() {
        return NetworkAddress.format(address);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }
}
