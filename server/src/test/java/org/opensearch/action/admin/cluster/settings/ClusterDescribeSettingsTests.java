/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.Filtered;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

public class ClusterDescribeSettingsTests extends OpenSearchTestCase {
    public void testDoesNotEvaluateDefault() {
        var calls = new AtomicInteger();
        Setting<String> setting = new Setting<>("plugin.lazy", settings -> {
            calls.incrementAndGet();
            return "default-value";
        }, value -> value, NodeScope);
        var registry = new ClusterSettings(Settings.EMPTY, Set.of(setting));
        int callsBeforeDescription = calls.get();
        var response = TransportClusterDescribeSettingsAction.describe(
            new ClusterDescribeSettingsRequest("plugin.lazy"),
            registry,
            new SettingsFilter(List.of())
        );
        assertEquals(Map.of("plugin.lazy", false), response.settings());
        assertEquals(callsBeforeDescription, calls.get());
    }

    public void testRegisteredDefinitionsAndFiltering() {
        var dynamic = Setting.boolSetting("plugin.dynamic", false, Dynamic, NodeScope);
        var fixed = Setting.boolSetting("plugin.static", false, NodeScope);
        var secret = Setting.simpleString("plugin.secret", NodeScope, Filtered);
        var filtered = Setting.simpleString("plugin.filtered", NodeScope);
        var affix = Setting.affixKeySetting("plugin.remote.", "enabled", key -> Setting.boolSetting(key, false, Dynamic, NodeScope));
        var registry = new ClusterSettings(Settings.EMPTY, Set.of(dynamic, fixed, secret, filtered, affix));
        var request = new ClusterDescribeSettingsRequest(
            "plugin.dynamic",
            "plugin.static",
            "plugin.secret",
            "plugin.filtered",
            "plugin.remote.analytics.enabled",
            "plugin.unknown",
            "plugin.dynamic"
        );
        assertNull(request.validate());
        var response = TransportClusterDescribeSettingsAction.describe(request, registry, new SettingsFilter(List.of("plugin.filtered*")));
        assertEquals(Map.of("plugin.dynamic", true, "plugin.static", false, "plugin.remote.analytics.enabled", true), response.settings());
        assertEquals(
            Map.of(),
            TransportClusterDescribeSettingsAction.describe(
                new ClusterDescribeSettingsRequest("plugin.secret", "plugin.unknown"),
                registry,
                new SettingsFilter(List.of())
            ).settings()
        );
    }

    public void testValidation() {
        assertNotNull(new ClusterDescribeSettingsRequest().validate());
        for (String name : List.of("", " ", "cluster.*", "cluster.?", "/regex/")) {
            assertNotNull(new ClusterDescribeSettingsRequest(name).validate());
        }
        assertNull(new ClusterDescribeSettingsRequest("cluster.max_shards_per_node").validate());
    }

    public void testSerialization() throws Exception {
        var request = new ClusterDescribeSettingsRequest("plugin.static", "plugin.dynamic");
        try (var out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                assertArrayEquals(request.names(), new ClusterDescribeSettingsRequest(in).names());
            }
        }
        var response = new ClusterDescribeSettingsResponse(Map.of("plugin.static", false, "plugin.dynamic", true));
        try (var out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                assertEquals(response.settings(), new ClusterDescribeSettingsResponse(in).settings());
            }
        }
    }
}
