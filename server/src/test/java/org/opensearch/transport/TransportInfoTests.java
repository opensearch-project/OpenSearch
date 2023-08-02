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

package org.opensearch.transport;

import org.opensearch.common.network.NetworkAddress;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

public class TransportInfoTests extends OpenSearchTestCase {

    private TransportInfo createTransportInfo(InetAddress address, int port, boolean cnameInPublishAddress) {
        BoundTransportAddress boundAddress = new BoundTransportAddress(
            new TransportAddress[] { new TransportAddress(address, port) },
            new TransportAddress(address, port)
        );
        Map<String, BoundTransportAddress> profiles = Collections.singletonMap("test_profile", boundAddress);
        return new TransportInfo(boundAddress, profiles, cnameInPublishAddress);
    }

    public void testCorrectlyDisplayPublishedCname() throws Exception {
        InetAddress address = InetAddress.getByName("localhost");
        int port = 9200;
        assertPublishAddress(createTransportInfo(address, port, true), "localhost/" + NetworkAddress.format(address) + ':' + port);
    }

    public void testHideCnameIfDeprecatedFormat() throws Exception {
        InetAddress address = InetAddress.getByName("localhost");
        int port = 9200;
        assertPublishAddress(createTransportInfo(address, port, false), NetworkAddress.format(address) + ':' + port);
        assertWarnings(
            "transport.publish_address was printed as [ip:port] instead of [hostname/ip:port]. "
                + "This format is deprecated and will change to [hostname/ip:port] in a future version. "
                + "Use -Dopensearch.transport.cname_in_publish_address=true to enforce non-deprecated formatting.",

            "transport.test_profile.publish_address was printed as [ip:port] instead of [hostname/ip:port]. "
                + "This format is deprecated and will change to [hostname/ip:port] in a future version. "
                + "Use -Dopensearch.transport.cname_in_publish_address=true to enforce non-deprecated formatting."
        );
    }

    public void testCorrectDisplayPublishedIp() throws Exception {
        InetAddress address = InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("localhost")));
        int port = 9200;
        assertPublishAddress(createTransportInfo(address, port, true), NetworkAddress.format(address) + ':' + port);
    }

    public void testCorrectDisplayPublishedIpv6() throws Exception {
        InetAddress address = InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("0:0:0:0:0:0:0:1")));
        int port = 9200;
        assertPublishAddress(createTransportInfo(address, port, true), new TransportAddress(address, port).toString());
    }

    @SuppressWarnings("unchecked")
    private void assertPublishAddress(TransportInfo httpInfo, String expected) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        httpInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> transportMap = (Map<String, Object>) createParser(builder).map().get(TransportInfo.Fields.TRANSPORT);
        Map<String, Object> profilesMap = (Map<String, Object>) transportMap.get("profiles");
        assertEquals(expected, transportMap.get(TransportInfo.Fields.PUBLISH_ADDRESS));
        assertEquals(expected, ((Map<String, Object>) profilesMap.get("test_profile")).get(TransportInfo.Fields.PUBLISH_ADDRESS));
    }
}
