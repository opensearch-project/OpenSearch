/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.node.EngineModeNodeAttribute.EngineMode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class EngineModeNodeAttributeTests extends OpenSearchTestCase {

    public void testDefaultsToLuceneWhenAttributeAbsent() throws UnknownHostException {
        DiscoveryNode node = makeNode(emptyMap());
        assertEquals(EngineMode.LUCENE, EngineModeNodeAttribute.get(node));
        assertEquals(EngineMode.LUCENE, EngineModeNodeAttribute.get(Settings.EMPTY));
        assertEquals(EngineMode.LUCENE, EngineModeNodeAttribute.DEFAULT);
    }

    public void testParsesKnownValues() throws UnknownHostException {
        assertEquals(EngineMode.LUCENE, EngineModeNodeAttribute.parse("lucene"));
        assertEquals(EngineMode.ANALYTICS, EngineModeNodeAttribute.parse("analytics"));
        // case-insensitive
        assertEquals(EngineMode.LUCENE, EngineModeNodeAttribute.parse("LUCENE"));
        assertEquals(EngineMode.ANALYTICS, EngineModeNodeAttribute.parse("Analytics"));

        DiscoveryNode node = makeNode(Map.of(EngineModeNodeAttribute.ENGINE_MODE_ATTRIBUTE_KEY, "analytics"));
        assertEquals(EngineMode.ANALYTICS, EngineModeNodeAttribute.get(node));
    }

    public void testReadsFromSettings() {
        Settings settings = Settings.builder().put(EngineModeNodeAttribute.ENGINE_MODE_SETTING_KEY, "analytics").build();
        assertEquals(EngineMode.ANALYTICS, EngineModeNodeAttribute.get(settings));
    }

    public void testRejectsUnknownValue() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> EngineModeNodeAttribute.parse("postgres"));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown"));
        assertTrue(e.getMessage(), e.getMessage().contains("engine_mode"));
        assertTrue(e.getMessage(), e.getMessage().contains("postgres"));
    }

    public void testValidateRejectsUnknownValue() {
        // valid values do not throw
        EngineModeNodeAttribute.validate("lucene");
        EngineModeNodeAttribute.validate("analytics");
        EngineModeNodeAttribute.validate("");

        expectThrows(IllegalArgumentException.class, () -> EngineModeNodeAttribute.validate("postgres"));
    }

    public void testNodeAttributesSettingValidatesEngineMode() {
        // Built through the actual NODE_ATTRIBUTES affix setting so we exercise the wired-in validator.
        Settings good = Settings.builder().put(EngineModeNodeAttribute.ENGINE_MODE_SETTING_KEY, "analytics").build();
        // get() runs the validator
        Node.NODE_ATTRIBUTES.getConcreteSetting(EngineModeNodeAttribute.ENGINE_MODE_SETTING_KEY).get(good);

        Settings bad = Settings.builder().put(EngineModeNodeAttribute.ENGINE_MODE_SETTING_KEY, "postgres").build();
        expectThrows(
            IllegalArgumentException.class,
            () -> Node.NODE_ATTRIBUTES.getConcreteSetting(EngineModeNodeAttribute.ENGINE_MODE_SETTING_KEY).get(bad)
        );
    }

    public void testSettingKeyMatchesNodeAttrPrefix() {
        assertEquals("node.attr.engine_mode", EngineModeNodeAttribute.ENGINE_MODE_SETTING_KEY);
        assertEquals("engine_mode", EngineModeNodeAttribute.ENGINE_MODE_ATTRIBUTE_KEY);
    }

    private static DiscoveryNode makeNode(Map<String, String> attributes) throws UnknownHostException {
        return new DiscoveryNode(
            "n1",
            new TransportAddress(InetAddress.getByName("localhost"), 9300),
            attributes,
            emptySet(),
            Version.CURRENT
        );
    }
}
