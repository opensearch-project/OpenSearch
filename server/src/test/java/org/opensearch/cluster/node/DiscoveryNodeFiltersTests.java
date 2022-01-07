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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.opensearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.hamcrest.Matchers.equalTo;

public class DiscoveryNodeFiltersTests extends OpenSearchTestCase {

    private static TransportAddress localAddress;

    @BeforeClass
    public static void createLocalAddress() throws UnknownHostException {
        localAddress = new TransportAddress(InetAddress.getByName("192.1.1.54"), 9999);
    }

    @AfterClass
    public static void releaseLocalAddress() {
        localAddress = null;
    }

    public void testNameMatch() {
        Settings settings = Settings.builder().put("xxx.name", "name1").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name2", "id2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIdMatch() {
        Settings settings = Settings.builder().put("xxx._id", "id1").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name2", "id2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIdOrNameMatch() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._id", "id1,blah").put("xxx.name", "blah,name2").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        final Version version = Version.CURRENT;
        DiscoveryNode node = new DiscoveryNode("name1", "id1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name2", "id2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name3", "id3", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testTagAndGroupMatch() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx.group", "B").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "B");
        DiscoveryNode node = new DiscoveryNode("name1", "id1", buildNewFakeTransportAddress(), attributes, emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "B");
        attributes.put("name", "X");
        node = new DiscoveryNode("name2", "id2", buildNewFakeTransportAddress(), attributes, emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "F");
        attributes.put("name", "X");
        node = new DiscoveryNode("name3", "id3", buildNewFakeTransportAddress(), attributes, emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));

        node = new DiscoveryNode("name4", "id4", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testStarMatch() {
        Settings settings = Settings.builder().put("xxx.name", "*").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringMatchingAnd() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "A").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringNotMatching() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "B").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpBindFilteringNotMatchingAnd() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "A").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "8.8.8.8").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpBindFilteringMatchingOr() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54").put("xxx.tag", "A").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringNotMatchingOr() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "A").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "8.8.8.8").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx._publish_ip", "192.1.1.54").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringNotMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx._publish_ip", "8.8.8.8").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpPublishFilteringMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._publish_ip", "192.1.1.54").put("xxx.tag", "A").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testHostNameFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._host", "A").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "A", "192.1.1.54", localAddress, emptyMap(), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testHostAddressFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._host", "192.1.1.54").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "A", "192.1.1.54", localAddress, emptyMap(), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringNotMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx._publish_ip", "8.8.8.8").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringMatchingWildcard() {
        boolean matches = randomBoolean();
        Settings settings = shuffleSettings(Settings.builder().put("xxx._publish_ip", matches ? "192.1.*" : "192.2.*").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, emptyMap(), emptySet(), null);
        assertThat(filters.match(node), equalTo(matches));
    }

    public void testCommaSeparatedValuesTrimmed() {
        DiscoveryNode node = new DiscoveryNode("", "", "", "", "192.1.1.54", localAddress, singletonMap("tag", "B"), emptySet(), null);

        Settings settings = shuffleSettings(
            Settings.builder()
                .put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.1, 192.1.1.54")
                .put("xxx.tag", "A, B")
                .build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);
        assertTrue(filters.match(node));
    }

    public void testAndNodeFiltersUpdate() {
        Settings settings = Settings.builder().put("xxx._id", "id1").build();
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        final DiscoveryNode node1 = new DiscoveryNode(
            "name1",
            "id1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        assertThat(filters.match(node1), equalTo(true));

        final DiscoveryNode node2 = new DiscoveryNode(
            "name2",
            "id2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        assertThat(filters.match(node2), equalTo(false));

        filters = buildOrUpdateFromSettings(filters, AND, "xxx.", Settings.builder().put("xxx.name", "name2").build());
        assertThat(filters.match(node1), equalTo(false));
        assertThat(filters.match(node2), equalTo(false));

        filters = buildOrUpdateFromSettings(filters, AND, "xxx.", Settings.builder().put("xxx._id", "").build());
        assertThat(filters.match(node1), equalTo(false));
        assertThat(filters.match(node2), equalTo(true));

        filters = buildOrUpdateFromSettings(filters, AND, "xxx.", Settings.builder().put("xxx.name", "name1").build());
        assertThat(filters.match(node1), equalTo(true));
        assertThat(filters.match(node2), equalTo(false));

        filters = buildOrUpdateFromSettings(filters, AND, "xxx.", Settings.builder().put("xxx.name", "").put("xxx._id", "id2").build());
        assertThat(filters.match(node1), equalTo(false));
        assertThat(filters.match(node2), equalTo(true));
    }

    public void testOrNodeFiltersUpdate() {
        Settings settings = Settings.builder().put("xxx._id", "id1").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        final DiscoveryNode node1 = new DiscoveryNode(
            "name1",
            "id1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        assertThat(filters.match(node1), equalTo(true));

        final DiscoveryNode node2 = new DiscoveryNode(
            "name2",
            "id2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        assertThat(filters.match(node2), equalTo(false));

        filters = buildOrUpdateFromSettings(filters, OR, "xxx.", Settings.builder().put("xxx.name", "name2").build());
        assertThat(filters.match(node1), equalTo(true));
        assertThat(filters.match(node2), equalTo(true));

        filters = buildOrUpdateFromSettings(filters, OR, "xxx.", Settings.builder().put("xxx._id", "").build());
        assertThat(filters.match(node1), equalTo(false));
        assertThat(filters.match(node2), equalTo(true));

        filters = buildOrUpdateFromSettings(filters, OR, "xxx.", Settings.builder().put("xxx.name", "name1").build());
        assertThat(filters.match(node1), equalTo(true));
        assertThat(filters.match(node2), equalTo(false));

        filters = buildOrUpdateFromSettings(filters, OR, "xxx.", Settings.builder().put("xxx.name", "name1,name2").build());
        assertThat(filters.match(node1), equalTo(true));
        assertThat(filters.match(node2), equalTo(true));
    }

    public void testOpTypeMismatch() {
        Settings settings = Settings.builder().put("xxx._id", "id1").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);
        try {
            buildOrUpdateFromSettings(filters, AND, "xxx.", Settings.builder().put("xxx.name", "name2").build());
        } catch (AssertionError error) {
            assertEquals("operation type should match with node filter parameter", error.getMessage());
        }
    }

    private Settings shuffleSettings(Settings source) {
        Settings.Builder settings = Settings.builder();
        List<String> keys = new ArrayList<>(source.keySet());
        Collections.shuffle(keys, random());
        for (String o : keys) {
            settings.put(o, source.get(o));
        }
        return settings.build();
    }

    public static DiscoveryNodeFilters buildFromSettings(DiscoveryNodeFilters.OpType opType, String prefix, Settings settings) {
        return buildOrUpdateFromSettings(null, opType, prefix, settings);
    }

    public static DiscoveryNodeFilters buildOrUpdateFromSettings(
        DiscoveryNodeFilters filters,
        DiscoveryNodeFilters.OpType opType,
        String prefix,
        Settings settings
    ) {
        Setting.AffixSetting<String> setting = Setting.prefixKeySetting(prefix, key -> Setting.simpleString(key));
        return DiscoveryNodeFilters.buildOrUpdateFromKeyValue(filters, opType, setting.getAsMap(settings));
    }
}
