/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.Filtered;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

public class ClusterDescribeSettingsTests extends OpenSearchTestCase {
    private static final SettingsFilter NO_FILTER = new SettingsFilter(List.of());

    private static ClusterDescribeSettingsResponse describe(ClusterSettings registry, SettingsFilter filter, String... names) {
        return TransportClusterDescribeSettingsAction.describe(new ClusterDescribeSettingsRequest(names), registry, filter);
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
        assertEquals(
            Map.of(
                "plugin.dynamic",
                SettingMetadata.withDefault(true, "false"),
                "plugin.static",
                SettingMetadata.withDefault(false, "false"),
                "plugin.remote.analytics.enabled",
                SettingMetadata.withDefault(true, "false")
            ),
            response.settings()
        );
        // Filtered and unknown names are both omitted, so neither can be told apart from the other.
        assertEquals(Map.of(), describe(registry, NO_FILTER, "plugin.secret", "plugin.unknown").settings());
    }

    public void testDefaultsUseDeclaredStringForm() {
        var count = Setting.intSetting("plugin.count", 1000, 1, Dynamic, NodeScope);
        var size = Setting.byteSizeSetting(
            "plugin.size",
            new ByteSizeValue(40, org.opensearch.core.common.unit.ByteSizeUnit.MB),
            NodeScope
        );
        var interval = Setting.timeSetting("plugin.interval", TimeValue.timeValueSeconds(30), NodeScope);
        var name = Setting.simpleString("plugin.name", "shard-aware", NodeScope);
        var registry = new ClusterSettings(Settings.EMPTY, Set.of(count, size, interval, name));
        var response = describe(registry, NO_FILTER, "plugin.count", "plugin.size", "plugin.interval", "plugin.name");
        // The default is the declared raw string, exactly as the setting itself parses it. Note that the unbounded
        // byteSizeSetting overload declares its default in bytes ("41943040b") while timeSetting uses the unit form
        // ("30s"); the API reports each as declared rather than re-rendering it.
        assertEquals(
            Map.of(
                "plugin.count",
                SettingMetadata.withDefault(true, "1000"),
                "plugin.size",
                SettingMetadata.withDefault(false, "41943040b"),
                "plugin.interval",
                SettingMetadata.withDefault(false, "30s"),
                "plugin.name",
                SettingMetadata.withDefault(false, "shard-aware")
            ),
            response.settings()
        );
    }

    public void testDependentDefaultIsResolvedAgainstEmptySettings() {
        var base = Setting.intSetting("plugin.base", 10, NodeScope);
        var derived = new Setting<>("plugin.derived", settings -> Integer.toString(base.get(settings) * 2), Integer::parseInt, NodeScope);
        // Even when the registry's node settings configure a different base, the declared default is reported.
        var registry = new ClusterSettings(Settings.builder().put("plugin.base", 500).build(), Set.of(base, derived));
        assertEquals(
            SettingMetadata.withDefault(false, "20"),
            describe(registry, NO_FILTER, "plugin.derived").settings().get("plugin.derived")
        );
    }

    public void testDefaultIsOmittedWhenNotMeaningful() {
        var secure = SecureSetting.secureString("plugin.password", null);
        var group = Setting.groupSetting("plugin.group.", NodeScope);
        var empty = Setting.simpleString("plugin.empty", NodeScope);
        var registry = new ClusterSettings(Settings.EMPTY, Set.of(secure, group, empty));
        var response = describe(registry, NO_FILTER, "plugin.password", "plugin.group.", "plugin.empty");
        // Secure settings are keystore-backed and have no plaintext default to report.
        assertEquals(SettingMetadata.withoutDefault(false), response.settings().get("plugin.password"));
        // Group settings declare an empty default, and an empty string default is never reported.
        assertEquals(SettingMetadata.withoutDefault(false), response.settings().get("plugin.group."));
        assertEquals(SettingMetadata.withoutDefault(false), response.settings().get("plugin.empty"));
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
        var response = new ClusterDescribeSettingsResponse(
            Map.of("plugin.static", SettingMetadata.withDefault(false, "false"), "plugin.dynamic", SettingMetadata.withoutDefault(true))
        );
        try (var out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                assertEquals(response.settings(), new ClusterDescribeSettingsResponse(in).settings());
            }
        }
    }

    public void testToXContentOmitsAbsentDefault() throws Exception {
        var response = new ClusterDescribeSettingsResponse(
            Map.of("plugin.with", SettingMetadata.withDefault(true, "5"), "plugin.without", SettingMetadata.withoutDefault(false))
        );
        String json = org.opensearch.core.common.Strings.toString(MediaTypeRegistry.JSON, response);
        assertEquals("{\"settings\":{\"plugin.with\":{\"dynamic\":true,\"default\":\"5\"},\"plugin.without\":{\"dynamic\":false}}}", json);
    }
}
