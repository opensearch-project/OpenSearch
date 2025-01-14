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

import org.opensearch.common.network.NetworkUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;

public class PublishPortTests extends OpenSearchTestCase {

    public void testPublishPort() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        boolean useProfile = randomBoolean();
        final String profile;
        Settings baseSettings;
        Settings settings;
        if (useProfile) {
            baseSettings = Settings.builder().put("transport.profiles.some_profile.port", 0).build();
            settings = randomBoolean() ? Settings.EMPTY : Settings.builder().put(TransportSettings.PUBLISH_PORT.getKey(), 9081).build();
            settings = Settings.builder().put(settings).put(baseSettings).put("transport.profiles.some_profile.publish_port", 9080).build();
            profile = "some_profile";

        } else {
            baseSettings = Settings.EMPTY;
            settings = Settings.builder().put(TransportSettings.PUBLISH_PORT.getKey(), 9081).build();
            settings = randomBoolean()
                ? settings
                : Settings.builder().put(settings).put("transport.profiles.default.publish_port", 9080).build();
            profile = "default";

        }

        int publishPort = Transport.resolvePublishPort(
            new TcpTransport.ProfileSettings(settings, profile).publishPort,
            randomAddresses(),
            getByName("127.0.0.2")
        );
        assertThat("Publish port should be explicitly set", publishPort, equalTo(useProfile ? 9080 : 9081));

        publishPort = Transport.resolvePublishPort(
            new TcpTransport.ProfileSettings(baseSettings, profile).publishPort,
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1")
        );
        assertThat("Publish port should be derived from matched address", publishPort, equalTo(boundPort));

        publishPort = Transport.resolvePublishPort(
            new TcpTransport.ProfileSettings(baseSettings, profile).publishPort,
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", boundPort)),
            getByName("127.0.0.3")
        );
        assertThat("Publish port should be derived from unique port of bound addresses", publishPort, equalTo(boundPort));

        int resPort = Transport.resolvePublishPort(
            new TcpTransport.ProfileSettings(baseSettings, profile).publishPort,
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.3")
        );
        assertThat("as publish_port not specified and non-unique port of bound addresses", resPort, equalTo(-1));

        publishPort = Transport.resolvePublishPort(
            new TcpTransport.ProfileSettings(baseSettings, profile).publishPort,
            asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1")
        );
        assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));

        if (NetworkUtils.SUPPORTS_V6) {
            publishPort = Transport.resolvePublishPort(
                new TcpTransport.ProfileSettings(baseSettings, profile).publishPort,
                asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
                getByName("::1")
            );
            assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));
        }
    }

    private InetSocketAddress address(String host, int port) throws UnknownHostException {
        return new InetSocketAddress(getByName(host), port);
    }

    private InetSocketAddress randomAddress() throws UnknownHostException {
        return address("127.0.0." + randomIntBetween(1, 100), randomIntBetween(9200, 9300));
    }

    private List<InetSocketAddress> randomAddresses() throws UnknownHostException {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            addresses.add(randomAddress());
        }
        return addresses;
    }
}
