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

package org.opensearch.cluster.node;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.test.NodeRoles;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.opensearch.test.NodeRoles.nonWarmNode;
import static org.opensearch.test.NodeRoles.remoteClusterClientNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class DiscoveryNodeTests extends OpenSearchTestCase {

    public void testRolesAreSorted() {
        final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        final DiscoveryNode node = new DiscoveryNode(
            "name",
            "id",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            emptyMap(),
            roles,
            Version.CURRENT
        );
        DiscoveryNodeRole previous = null;
        for (final DiscoveryNodeRole current : node.getRoles()) {
            if (previous != null) {
                assertThat(current, greaterThanOrEqualTo(previous));
            }
            previous = current;
        }

    }

    public void testRemoteStoreRedactionInToString() {
        final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        Map<String, String> attributes = new HashMap<>();
        attributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "test-repo");
        attributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "test-repo");
        final DiscoveryNode node = new DiscoveryNode(
            "name",
            "id",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            attributes,
            roles,
            Version.CURRENT
        );
        assertFalse(node.toString().contains(RemoteStoreNodeAttribute.REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.get(0)));
    }

    public void testDiscoveryNodeIsCreatedWithHostFromInetAddress() throws Exception {
        InetAddress inetAddress = randomBoolean()
            ? InetAddress.getByName("192.0.2.1")
            : InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);
        assertEquals(transportAddress.address().getHostString(), node.getHostName());
        assertEquals(transportAddress.getAddress(), node.getHostAddress());
    }

    public void testDiscoveryNodeSerializationKeepsHost() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.CURRENT);
        node.writeTo(streamOutput);

        StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
        DiscoveryNode serialized = new DiscoveryNode(in);
        assertEquals(transportAddress.address().getHostString(), serialized.getHostName());
        assertEquals(transportAddress.address().getHostString(), serialized.getAddress().address().getHostString());
        assertEquals(transportAddress.getAddress(), serialized.getHostAddress());
        assertEquals(transportAddress.getAddress(), serialized.getAddress().getAddress());
        assertEquals(transportAddress.getPort(), serialized.getAddress().getPort());
    }

    public void testWriteVerifiableTo() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        Map<String, String> attributes = new HashMap<>();
        attributes.put("att-1", "test-repo");
        attributes.put("att-2", "test-repo");
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, attributes, roles, Version.CURRENT);

        BytesStreamOutput out = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out);
        node.writeVerifiableTo(checksumOut);
        StreamInput in = out.bytes().streamInput();
        DiscoveryNode result = new DiscoveryNode(in);
        assertEquals(result, node);

        Map<String, String> attributes2 = new HashMap<>();
        attributes2.put("att-2", "test-repo");
        attributes2.put("att-1", "test-repo");

        DiscoveryNode node2 = new DiscoveryNode(
            node.getName(),
            node.getId(),
            node.getEphemeralId(),
            node.getHostName(),
            node.getHostAddress(),
            transportAddress,
            attributes2,
            roles.stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new)),
            Version.CURRENT
        );
        BytesStreamOutput out2 = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut2 = new BufferedChecksumStreamOutput(out2);
        node2.writeVerifiableTo(checksumOut2);
        assertEquals(checksumOut.getChecksum(), checksumOut2.getChecksum());
    }

    public void testDiscoveryNodeRoleWithOldVersion() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

        DiscoveryNodeRole customRole = new DiscoveryNodeRole("custom_role", "z", true) {
            @Override
            public Setting<Boolean> legacySetting() {
                return null;
            }

            @Override
            public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
                if (nodeVersion.equals(Version.CURRENT)) {
                    return this;
                } else {
                    return DiscoveryNodeRole.DATA_ROLE;
                }
            }
        };

        DiscoveryNode node = new DiscoveryNode(
            "name1",
            "id1",
            transportAddress,
            emptyMap(),
            Collections.singleton(customRole),
            Version.CURRENT
        );

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setVersion(Version.CURRENT);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setVersion(Version.CURRENT);
            DiscoveryNode serialized = new DiscoveryNode(in);
            assertThat(
                serialized.getRoles().stream().map(DiscoveryNodeRole::roleName).collect(Collectors.joining()),
                equalTo("custom_role")
            );
        }

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setVersion(Version.V_2_0_0);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setVersion(Version.V_2_0_0);
            DiscoveryNode serialized = new DiscoveryNode(in);
            assertThat(serialized.getRoles().stream().map(DiscoveryNodeRole::roleName).collect(Collectors.joining()), equalTo("data"));
        }

    }

    public void testDiscoveryNodeIsRemoteClusterClientDefault() {
        runTestDiscoveryNodeIsRemoteClusterClient(Settings.EMPTY, true);
    }

    public void testDiscoveryNodeIsRemoteClusterClientSet() {
        runTestDiscoveryNodeIsRemoteClusterClient(remoteClusterClientNode(), true);
    }

    public void testDiscoveryNodeIsRemoteClusterClientUnset() {
        runTestDiscoveryNodeIsRemoteClusterClient(nonRemoteClusterClientNode(), false);
    }

    public void testDiscoveryNodeIsWarmSet() {
        runTestDiscoveryNodeIsWarm(NodeRoles.warmNode(), true);
    }

    public void testDiscoveryNodeIsWarmUnset() {
        runTestDiscoveryNodeIsWarm(nonWarmNode(), false);
    }

    public void testDiscoveryNodeIsSearchSet() {
        runTestDiscoveryNodeIsSearch(NodeRoles.searchOnlyNode(), true);
    }

    public void testDiscoveryNodeIsSearchUnset() {
        runTestDiscoveryNodeIsSearch(NodeRoles.nonSearchNode(), false);
    }

    // Added in 2.0 temporarily, validate the MASTER_ROLE is in the list of known roles.
    // MASTER_ROLE was removed from BUILT_IN_ROLES and is imported by setDeprecatedMasterRole(),
    // as a workaround for making the new CLUSTER_MANAGER_ROLE has got the same abbreviation 'm'.
    // The test validate this behavior.
    public void testSetDeprecatedMasterRoleCanAddMasterRole() {
        DiscoveryNode.setDeprecatedMasterRole();
        assertTrue(DiscoveryNode.getPossibleRoleNames().contains(DiscoveryNodeRole.MASTER_ROLE.roleName()));
    }

    private void runTestDiscoveryNodeIsRemoteClusterClient(final Settings settings, final boolean expected) {
        final DiscoveryNode node = DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node");
        assertThat(node.isRemoteClusterClient(), equalTo(expected));
        if (expected) {
            assertThat(node.getRoles(), hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
        } else {
            assertThat(node.getRoles(), not(hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)));
        }
    }

    private void runTestDiscoveryNodeIsWarm(final Settings settings, final boolean expected) {
        final DiscoveryNode node = DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node");
        assertThat(node.isWarmNode(), equalTo(expected));
        if (expected) {
            assertThat(node.getRoles(), hasItem(DiscoveryNodeRole.WARM_ROLE));
        } else {
            assertThat(node.getRoles(), not(hasItem(DiscoveryNodeRole.WARM_ROLE)));
        }
    }

    private void runTestDiscoveryNodeIsSearch(final Settings settings, final boolean expected) {
        final DiscoveryNode node = DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node");
        assertThat(node.isSearchNode(), equalTo(expected));
        if (expected) {
            assertThat(node.getRoles(), hasItem(DiscoveryNodeRole.SEARCH_ROLE));
        } else {
            assertThat(node.getRoles(), not(hasItem(DiscoveryNodeRole.SEARCH_ROLE)));
        }
    }

    public void testGetRoleFromRoleNameIsCaseInsensitive() {
        String dataRoleName = "DATA";
        DiscoveryNodeRole dataNodeRole = DiscoveryNode.getRoleFromRoleName(dataRoleName);
        assertEquals(DiscoveryNodeRole.DATA_ROLE, dataNodeRole);

        String dynamicRoleName = "TestRole";
        DiscoveryNodeRole dynamicNodeRole = DiscoveryNode.getRoleFromRoleName(dynamicRoleName);
        assertEquals(dynamicRoleName.toLowerCase(Locale.ROOT), dynamicNodeRole.roleName());
        assertEquals(dynamicRoleName.toLowerCase(Locale.ROOT), dynamicNodeRole.roleNameAbbreviation());
    }

    public void testDiscoveryNodeIsWarmNode() {
        final Settings settingWithWarmRole = NodeRoles.onlyRole(DiscoveryNodeRole.WARM_ROLE);
        final DiscoveryNode node = DiscoveryNode.createLocal(settingWithWarmRole, buildNewFakeTransportAddress(), "node");
        assertThat(node.isWarmNode(), equalTo(true));
    }
}
