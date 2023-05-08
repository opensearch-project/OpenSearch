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

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.Nullable;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.transport.ProtobufBoundTransportAddress;
import org.opensearch.common.transport.ProtobufTransportAddress;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.Booleans.parseBoolean;

/**
 * Transport information
*
* @opensearch.internal
*/
public class ProtobufTransportInfo implements ProtobufReportingService.ProtobufInfo {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportInfo.class);

    /** Whether to add hostname to publish host field when serializing. */
    private static final boolean CNAME_IN_PUBLISH_ADDRESS = parseBoolean(
        System.getProperty("opensearch.transport.cname_in_publish_address"),
        false
    );

    private final ProtobufBoundTransportAddress address;
    private Map<String, ProtobufBoundTransportAddress> profileAddresses;
    private final boolean cnameInPublishAddress;

    public ProtobufTransportInfo(
        ProtobufBoundTransportAddress address,
        @Nullable Map<String, ProtobufBoundTransportAddress> profileAddresses
    ) {
        this(address, profileAddresses, CNAME_IN_PUBLISH_ADDRESS);
    }

    public ProtobufTransportInfo(
        ProtobufBoundTransportAddress address,
        @Nullable Map<String, ProtobufBoundTransportAddress> profileAddresses,
        boolean cnameInPublishAddress
    ) {
        this.address = address;
        this.profileAddresses = profileAddresses;
        this.cnameInPublishAddress = cnameInPublishAddress;
    }

    public ProtobufTransportInfo(CodedInputStream in) throws IOException {
        address = new ProtobufBoundTransportAddress(in);
        int size = in.readInt32();
        if (size > 0) {
            profileAddresses = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                ProtobufBoundTransportAddress value = new ProtobufBoundTransportAddress(in);
                profileAddresses.put(key, value);
            }
        }
        this.cnameInPublishAddress = CNAME_IN_PUBLISH_ADDRESS;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        address.writeTo(out);
        if (profileAddresses != null) {
            out.writeInt32NoTag(profileAddresses.size());
        } else {
            out.writeInt32NoTag(0);
        }
        if (profileAddresses != null && profileAddresses.size() > 0) {
            for (Map.Entry<String, ProtobufBoundTransportAddress> entry : profileAddresses.entrySet()) {
                out.writeStringNoTag(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String BOUND_ADDRESS = "bound_address";
        static final String PUBLISH_ADDRESS = "publish_address";
        static final String PROFILES = "profiles";
    }

    private String formatPublishAddressString(String propertyName, ProtobufTransportAddress publishAddress) {
        String publishAddressString = publishAddress.toString();
        String hostString = publishAddress.address().getHostString();
        if (InetAddresses.isInetAddress(hostString) == false) {
            if (cnameInPublishAddress) {
                publishAddressString = hostString + '/' + publishAddress.toString();
            } else {
                deprecationLogger.deprecate(
                    "cname_in_publish_address_" + propertyName,
                    propertyName
                        + " was printed as [ip:port] instead of [hostname/ip:port]. "
                        + "This format is deprecated and will change to [hostname/ip:port] in a future version. "
                        + "Use -Dopensearch.transport.cname_in_publish_address=true to enforce non-deprecated formatting."
                );
            }
        }
        return publishAddressString;
    }

    public ProtobufBoundTransportAddress address() {
        return address;
    }

    public ProtobufBoundTransportAddress getAddress() {
        return address();
    }

    public Map<String, ProtobufBoundTransportAddress> getProfileAddresses() {
        return profileAddresses();
    }

    public Map<String, ProtobufBoundTransportAddress> profileAddresses() {
        return profileAddresses;
    }
}
