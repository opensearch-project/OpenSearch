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

import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import static org.opensearch.test.NodeRoles.nonClusterManagerNode;
import static org.opensearch.test.NodeRoles.nonDataNode;
import static org.opensearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.equalTo;

public class ConnectionProfileTests extends OpenSearchTestCase {

    public void testBuildConnectionProfile() {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        TimeValue connectTimeout = TimeValue.timeValueMillis(randomIntBetween(1, 10));
        TimeValue handshakeTimeout = TimeValue.timeValueMillis(randomIntBetween(1, 10));
        TimeValue pingInterval = TimeValue.timeValueMillis(randomIntBetween(1, 10));
        boolean compressionEnabled = randomBoolean();
        final boolean setConnectTimeout = randomBoolean();
        if (setConnectTimeout) {
            builder.setConnectTimeout(connectTimeout);
        }
        final boolean setHandshakeTimeout = randomBoolean();
        if (setHandshakeTimeout) {
            builder.setHandshakeTimeout(handshakeTimeout);
        }
        final boolean setCompress = randomBoolean();
        if (setCompress) {
            builder.setCompressionEnabled(compressionEnabled);
        }
        final boolean setPingInterval = randomBoolean();
        if (setPingInterval) {
            builder.setPingInterval(pingInterval);
        }
        builder.addConnections(1, TransportRequestOptions.Type.BULK);
        builder.addConnections(2, TransportRequestOptions.Type.STATE, TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(3, TransportRequestOptions.Type.PING);

        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, builder::build);
        assertEquals(
            "not all types are added for this connection profile - missing types: [REG, STREAM]",
            illegalStateException.getMessage()
        );

        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> builder.addConnections(4, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING)
        );
        assertEquals("type [PING] is already registered", illegalArgumentException.getMessage());
        builder.addConnections(4, TransportRequestOptions.Type.REG);
        builder.addConnections(1, TransportRequestOptions.Type.STREAM);
        ConnectionProfile build = builder.build();
        if (randomBoolean()) {
            build = new ConnectionProfile.Builder(build).build();
        }
        assertEquals(11, build.getNumConnections());
        if (setConnectTimeout) {
            assertEquals(connectTimeout, build.getConnectTimeout());
        } else {
            assertNull(build.getConnectTimeout());
        }

        if (setHandshakeTimeout) {
            assertEquals(handshakeTimeout, build.getHandshakeTimeout());
        } else {
            assertNull(build.getHandshakeTimeout());
        }

        if (setCompress) {
            assertEquals(compressionEnabled, build.getCompressionEnabled());
        } else {
            assertNull(build.getCompressionEnabled());
        }

        if (setPingInterval) {
            assertEquals(pingInterval, build.getPingInterval());
        } else {
            assertNull(build.getPingInterval());
        }

        List<Integer> list = new ArrayList<>(11);
        for (int i = 0; i < 11; i++) {
            list.add(i);
        }
        final int numIters = randomIntBetween(5, 10);
        assertEquals(5, build.getHandles().size());
        assertEquals(0, build.getHandles().get(0).offset);
        assertEquals(1, build.getHandles().get(0).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.BULK), build.getHandles().get(0).getTypes());
        Integer channel = build.getHandles().get(0).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertEquals(0, channel.intValue());
        }

        assertEquals(1, build.getHandles().get(1).offset);
        assertEquals(2, build.getHandles().get(1).length);
        assertEquals(
            EnumSet.of(TransportRequestOptions.Type.STATE, TransportRequestOptions.Type.RECOVERY),
            build.getHandles().get(1).getTypes()
        );
        channel = build.getHandles().get(1).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertThat(channel, Matchers.anyOf(Matchers.is(1), Matchers.is(2)));
        }

        assertEquals(3, build.getHandles().get(2).offset);
        assertEquals(3, build.getHandles().get(2).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.PING), build.getHandles().get(2).getTypes());
        channel = build.getHandles().get(2).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertThat(channel, Matchers.anyOf(Matchers.is(3), Matchers.is(4), Matchers.is(5)));
        }

        assertEquals(6, build.getHandles().get(3).offset);
        assertEquals(4, build.getHandles().get(3).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.REG), build.getHandles().get(3).getTypes());
        channel = build.getHandles().get(3).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertThat(channel, Matchers.anyOf(Matchers.is(6), Matchers.is(7), Matchers.is(8), Matchers.is(9)));
        }

        assertEquals(10, build.getHandles().get(4).offset);
        assertEquals(1, build.getHandles().get(4).length);
        assertEquals(EnumSet.of(TransportRequestOptions.Type.STREAM), build.getHandles().get(4).getTypes());
        channel = build.getHandles().get(4).getChannel(list);
        for (int i = 0; i < numIters; i++) {
            assertEquals(10, channel.intValue());
        }

        assertEquals(3, build.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(4, build.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(2, build.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, build.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(1, build.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
        assertEquals(1, build.getNumConnectionsPerType(TransportRequestOptions.Type.STREAM));
    }

    public void testNoChannels() {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(
            1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.STATE,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STREAM
        );
        builder.addConnections(0, TransportRequestOptions.Type.PING);
        ConnectionProfile build = builder.build();
        List<Integer> array = Collections.singletonList(0);
        assertEquals(Integer.valueOf(0), build.getHandles().get(0).getChannel(array));
        expectThrows(IllegalStateException.class, () -> build.getHandles().get(1).getChannel(array));
    }

    public void testConnectionProfileResolve() {
        final ConnectionProfile defaultProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        assertEquals(defaultProfile, ConnectionProfile.resolveConnectionProfile(null, defaultProfile));

        final ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.BULK);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.REG);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.STATE);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.PING);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.STREAM);

        final boolean connectionTimeoutSet = randomBoolean();
        if (connectionTimeoutSet) {
            builder.setConnectTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        final boolean connectionHandshakeSet = randomBoolean();
        if (connectionHandshakeSet) {
            builder.setHandshakeTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        final boolean pingIntervalSet = randomBoolean();
        if (pingIntervalSet) {
            builder.setPingInterval(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        final boolean connectionCompressSet = randomBoolean();
        if (connectionCompressSet) {
            builder.setCompressionEnabled(randomBoolean());
        }

        final ConnectionProfile profile = builder.build();
        final ConnectionProfile resolved = ConnectionProfile.resolveConnectionProfile(profile, defaultProfile);
        assertNotEquals(resolved, defaultProfile);
        assertThat(resolved.getNumConnections(), equalTo(profile.getNumConnections()));
        assertThat(resolved.getHandles(), equalTo(profile.getHandles()));

        assertThat(
            resolved.getConnectTimeout(),
            equalTo(connectionTimeoutSet ? profile.getConnectTimeout() : defaultProfile.getConnectTimeout())
        );
        assertThat(
            resolved.getHandshakeTimeout(),
            equalTo(connectionHandshakeSet ? profile.getHandshakeTimeout() : defaultProfile.getHandshakeTimeout())
        );
        assertThat(resolved.getPingInterval(), equalTo(pingIntervalSet ? profile.getPingInterval() : defaultProfile.getPingInterval()));
        assertThat(
            resolved.getCompressionEnabled(),
            equalTo(connectionCompressSet ? profile.getCompressionEnabled() : defaultProfile.getCompressionEnabled())
        );
    }

    public void testDefaultConnectionProfile() {
        ConnectionProfile profile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        assertEquals(13, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STREAM));
        assertEquals(TransportSettings.CONNECT_TIMEOUT.get(Settings.EMPTY), profile.getConnectTimeout());
        assertEquals(TransportSettings.CONNECT_TIMEOUT.get(Settings.EMPTY), profile.getHandshakeTimeout());
        assertEquals(TransportSettings.TRANSPORT_COMPRESS.get(Settings.EMPTY), profile.getCompressionEnabled());
        assertEquals(TransportSettings.PING_SCHEDULE.get(Settings.EMPTY), profile.getPingInterval());

        profile = ConnectionProfile.buildDefaultConnectionProfile(nonClusterManagerNode());
        assertEquals(12, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STREAM));

        profile = ConnectionProfile.buildDefaultConnectionProfile(nonDataNode());
        assertEquals(11, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STREAM));

        profile = ConnectionProfile.buildDefaultConnectionProfile(
            removeRoles(
                Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)))
            )
        );
        assertEquals(10, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STREAM));
    }
}
