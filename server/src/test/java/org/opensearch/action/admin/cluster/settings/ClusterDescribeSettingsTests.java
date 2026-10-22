/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingBounds;
import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeUnit;
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
                SettingMetadata.withDefault(true, "false").withValidation(SettingBounds.ofType("boolean")),
                "plugin.static",
                SettingMetadata.withDefault(false, "false").withValidation(SettingBounds.ofType("boolean")),
                "plugin.remote.analytics.enabled",
                SettingMetadata.withDefault(true, "false").withValidation(SettingBounds.ofType("boolean"))
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
                SettingMetadata.withDefault(true, "1000").withValidation(SettingBounds.of("integer", "1", null)),
                "plugin.size",
                SettingMetadata.withDefault(false, "41943040b").withValidation(SettingBounds.ofType("byte_size")),
                "plugin.interval",
                SettingMetadata.withDefault(false, "30s"),
                "plugin.name",
                SettingMetadata.withDefault(false, "shard-aware")
            ),
            response.settings()
        );
    }

    public void testBoundsOfBoundedParsers() {
        assertEquals(
            SettingBounds.of("integer", "1", "10"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.intSetting("p.int", 5, 1, 10, NodeScope))
        );
        assertEquals(
            SettingBounds.of("long", "2", null),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.longSetting("p.long", 5L, 2L, NodeScope))
        );
        assertEquals(
            SettingBounds.of("float", "0.5", null),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.floatSetting("p.float", 1.5f, 0.5f, NodeScope))
        );
        assertEquals(
            SettingBounds.of("double", "0.5", "2.0"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.doubleSetting("p.double", 1.5, 0.5, 2.0, NodeScope))
        );
        assertEquals(
            SettingBounds.of("byte_size", "1kb", "1gb"),
            TransportClusterDescribeSettingsAction.boundsOf(
                Setting.byteSizeSetting(
                    "p.bytes",
                    new ByteSizeValue(1, ByteSizeUnit.MB),
                    new ByteSizeValue(1, ByteSizeUnit.KB),
                    new ByteSizeValue(1, ByteSizeUnit.GB),
                    NodeScope
                )
            )
        );
        // Bytes may be bounded on one side only; -1 bytes is the "no minimum" sentinel.
        assertEquals(
            SettingBounds.of("byte_size", null, "1gb"),
            TransportClusterDescribeSettingsAction.boundsOf(
                Setting.byteSizeSetting(
                    "p.bytes.max",
                    new ByteSizeValue(1, ByteSizeUnit.MB),
                    new ByteSizeValue(-1),
                    new ByteSizeValue(1, ByteSizeUnit.GB),
                    NodeScope
                )
            )
        );
        assertEquals(
            SettingBounds.of("time", "0ms", null),
            TransportClusterDescribeSettingsAction.boundsOf(
                Setting.positiveTimeSetting("p.time.min", TimeValue.timeValueSeconds(5), NodeScope)
            )
        );
        assertEquals(
            SettingBounds.of("time", "1s", "1m"),
            TransportClusterDescribeSettingsAction.boundsOf(
                Setting.timeSetting(
                    "p.time.minmax",
                    TimeValue.timeValueSeconds(5),
                    TimeValue.timeValueSeconds(1),
                    TimeValue.timeValueMinutes(1),
                    NodeScope
                )
            )
        );
        // A concrete affix key resolves to its delegate's bounded parser.
        var affix = Setting.affixKeySetting("p.remote.", "count", key -> Setting.intSetting(key, 1, 0, 100, NodeScope));
        assertEquals(
            SettingBounds.of("integer", "0", "100"),
            TransportClusterDescribeSettingsAction.boundsOf(affix.getConcreteSettingForNamespace("ns"))
        );
    }

    public void testBoundsOfReportsTypeWithoutBoundsWhenSentinelsAreUsed() {
        // The unbounded numeric factories delegate to the ranged overload with the type's MIN/MAX as sentinels; the type is
        // still known, so it is reported, but the sentinels are not.
        assertEquals(
            SettingBounds.ofType("integer"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.intSetting("p.int", 5, NodeScope))
        );
        assertEquals(
            SettingBounds.ofType("long"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.longSetting("p.long", 5L, NodeScope))
        );
        assertEquals(
            SettingBounds.ofType("float"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.floatSetting("p.float", 1.5f, NodeScope))
        );
        assertEquals(
            SettingBounds.ofType("double"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.doubleSetting("p.double", 1.5, NodeScope))
        );
        assertEquals(
            SettingBounds.ofType("byte_size"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.byteSizeSetting("p.bytes", new ByteSizeValue(1), NodeScope))
        );
        // Upper bounds declared on their own are kept.
        assertEquals(
            SettingBounds.of("long", null, "9"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.longSetting("p.long.max", 5L, Long.MIN_VALUE, 9L, NodeScope))
        );
        assertEquals(
            SettingBounds.of("float", null, "9.0"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.floatSetting("p.float.max", 1.5f, Float.MIN_VALUE, 9.0f, NodeScope))
        );
    }

    public void testBoundsOfTypeOnlyParsers() {
        // Boolean settings accept only true/false; the type is the whole constraint.
        assertEquals(
            SettingBounds.ofType("boolean"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.boolSetting("p.bool", false, NodeScope))
        );
        assertEquals(
            SettingBounds.ofType("boolean"),
            TransportClusterDescribeSettingsAction.boundsOf(
                Setting.boolSetting("p.bool.fallback", Setting.boolSetting("p.bool", false, NodeScope), NodeScope)
            )
        );
        // Memory size accepts an absolute byte size or a percentage of heap; the percentage form is bounded to [0, 100].
        assertEquals(
            SettingBounds.of("memory_size", "0%", "100%"),
            TransportClusterDescribeSettingsAction.boundsOf(Setting.memorySizeSetting("p.memory", "50%", NodeScope))
        );
        assertEquals(
            SettingBounds.of("memory_size", "0%", "100%"),
            TransportClusterDescribeSettingsAction.boundsOf(
                Setting.memorySizeSetting("p.memory.bytes", new ByteSizeValue(1, ByteSizeUnit.GB), NodeScope)
            )
        );
    }

    public void testBoundsOfReturnsNullForOpaqueParsers() {
        // These factories wrap a lambda; neither the type nor any bound can be read from it.
        assertNull(
            TransportClusterDescribeSettingsAction.boundsOf(Setting.timeSetting("p.time", TimeValue.timeValueSeconds(1), NodeScope))
        );
        assertNull(TransportClusterDescribeSettingsAction.boundsOf(Setting.simpleString("p.string", NodeScope)));
        assertNull(
            TransportClusterDescribeSettingsAction.boundsOf(
                new Setting<>("p.enum", "all", s -> s.toUpperCase(java.util.Locale.ROOT), NodeScope)
            )
        );
        assertNull(TransportClusterDescribeSettingsAction.boundsOf(SecureSetting.secureString("p.secret", null)));
    }

    public void testEveryBuiltInSettingDescribesWithoutError() throws Exception {
        // Exhaustive guard against null bounds or unexpected parser shapes. boundsOf runs over every cluster and index
        // setting core registers; the full describe/render path runs over every cluster setting (index settings
        // cannot be placed in a ClusterSettings registry). Affix templates are skipped: describe() only sees concrete keys.
        var all = new java.util.ArrayList<Setting<?>>();
        all.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        all.addAll(org.opensearch.common.settings.IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        int bounded = 0;
        for (Setting<?> setting : all) {
            if (setting instanceof Setting.AffixSetting) {
                continue;
            }
            SettingBounds bounds = TransportClusterDescribeSettingsAction.boundsOf(setting);
            if (bounds != null) {
                bounded++;
                assertNotNull(setting.getKey(), bounds.type());
                // A sentinel leaking through would surface as one of these strings. "-1" is only a sentinel for byte sizes;
                // time settings legitimately declare -1 ("disabled") as their minimum and must report it.
                for (String sentinel : List.of(
                    Integer.toString(Integer.MIN_VALUE),
                    Integer.toString(Integer.MAX_VALUE),
                    Long.toString(Long.MIN_VALUE),
                    Long.toString(Long.MAX_VALUE),
                    Float.toString(Float.MIN_VALUE),
                    Float.toString(Float.MAX_VALUE),
                    Double.toString(Double.MIN_VALUE),
                    Double.toString(Double.MAX_VALUE)
                )) {
                    assertNotEquals(setting.getKey() + " min", sentinel, bounds.min());
                    assertNotEquals(setting.getKey() + " max", sentinel, bounds.max());
                }
                if ("byte_size".equals(bounds.type())) {
                    assertNotEquals(setting.getKey() + " min", "-1", bounds.min());
                }
            }
        }
        assertTrue("expected some built-in settings to declare bounds, got " + bounded, bounded > 50);

        int described = 0;
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            if (setting instanceof Setting.AffixSetting) {
                continue;
            }
            var registry = new ClusterSettings(Settings.EMPTY, Set.of(setting));
            var response = describe(registry, NO_FILTER, setting.getKey());
            assertEquals(setting.getKey(), 1, response.settings().size());
            assertNotNull(org.opensearch.core.common.Strings.toString(MediaTypeRegistry.JSON, response));
            described++;
        }
        logger.info(
            "--> boundsOf over {} built-in settings ({} with declared bounds); full describe path over {}",
            all.size(),
            bounded,
            described
        );
    }

    public void testValidationFlowsThroughDescribeAndFilterGate() {
        var bounded = Setting.intSetting("plugin.bounded", 5, 1, 10, Dynamic, NodeScope);
        var unbounded = Setting.intSetting("plugin.unbounded", 5, NodeScope);
        var hidden = Setting.intSetting("plugin.hidden", 5, 1, 10, NodeScope, Filtered);
        var registry = new ClusterSettings(Settings.EMPTY, Set.of(bounded, unbounded, hidden));
        var response = describe(registry, NO_FILTER, "plugin.bounded", "plugin.unbounded", "plugin.hidden");
        assertEquals(
            Map.of(
                "plugin.bounded",
                SettingMetadata.withDefault(true, "5").withValidation(SettingBounds.of("integer", "1", "10")),
                "plugin.unbounded",
                SettingMetadata.withDefault(false, "5").withValidation(SettingBounds.ofType("integer"))
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
            Map.of(
                "plugin.static",
                SettingMetadata.withDefault(false, "false"),
                "plugin.dynamic",
                SettingMetadata.withoutDefault(true),
                "plugin.bounded",
                SettingMetadata.withDefault(true, "5").withValidation(SettingBounds.of("integer", "1", null)),
                "plugin.capped",
                SettingMetadata.withoutDefault(false).withValidation(SettingBounds.of("byte_size", null, "1gb"))
            )
        );
        // The response is local-only: it is rendered with toXContent on the receiving node and never put on the wire.
        expectThrows(UnsupportedOperationException.class, () -> response.writeTo(new BytesStreamOutput()));
        expectThrows(UnsupportedOperationException.class, () -> new ClusterDescribeSettingsResponse((StreamInput) null));
    }

    public void testValueObjectContracts() {
        var bounds = SettingBounds.of("integer", "1", "10");
        assertEquals("integer", bounds.type());
        assertEquals("1", bounds.min());
        assertEquals("10", bounds.max());
        assertEquals(bounds, SettingBounds.of("integer", "1", "10"));
        assertEquals(bounds.hashCode(), SettingBounds.of("integer", "1", "10").hashCode());
        assertNotEquals(bounds, SettingBounds.of("integer", "1", null));
        assertNotEquals(bounds, SettingBounds.of("long", "1", "10"));
        assertNotEquals(bounds, null);
        assertNotEquals(bounds, "integer");
        assertEquals("SettingBounds{type=integer, min=1, max=10}", bounds.toString());

        var metadata = SettingMetadata.withDefault(true, "5").withValidation(bounds);
        assertTrue(metadata.dynamic());
        assertEquals("5", metadata.defaultValue());
        assertEquals(bounds, metadata.validation());
        assertNull(SettingMetadata.withoutDefault(false).defaultValue());
        assertNull(SettingMetadata.withoutDefault(false).validation());
        assertEquals(bounds, bounds);
        assertEquals(metadata, metadata);
        assertEquals(SettingBounds.ofType("boolean"), SettingBounds.of("boolean", null, null));
        assertNull(SettingBounds.ofType("boolean").min());
        assertNull(SettingBounds.ofType("boolean").max());
        assertEquals(metadata, SettingMetadata.withDefault(true, "5").withValidation(SettingBounds.of("integer", "1", "10")));
        assertEquals(metadata.hashCode(), SettingMetadata.withDefault(true, "5").withValidation(bounds).hashCode());
        assertNotEquals(metadata, SettingMetadata.withDefault(true, "5"));
        assertNotEquals(metadata, SettingMetadata.withDefault(false, "5").withValidation(bounds));
        assertNotEquals(metadata, SettingMetadata.withDefault(true, "6").withValidation(bounds));
        assertNotEquals(metadata, null);
        assertNotEquals(metadata, bounds);
        assertEquals(
            "SettingMetadata{dynamic=true, default=5, validation=SettingBounds{type=integer, min=1, max=10}}",
            metadata.toString()
        );
        expectThrows(NullPointerException.class, () -> SettingMetadata.withDefault(true, null));
    }

    public void testToXContentOmitsAbsentDefault() throws Exception {
        var response = new ClusterDescribeSettingsResponse(
            Map.of(
                "plugin.with",
                SettingMetadata.withDefault(true, "5"),
                "plugin.without",
                SettingMetadata.withoutDefault(false),
                "plugin.bounded",
                SettingMetadata.withDefault(true, "5").withValidation(SettingBounds.of("integer", "1", null)),
                "plugin.flag",
                SettingMetadata.withDefault(false, "true").withValidation(SettingBounds.ofType("boolean"))
            )
        );
        String json = org.opensearch.core.common.Strings.toString(MediaTypeRegistry.JSON, response);
        assertEquals(
            "{\"settings\":{"
                + "\"plugin.bounded\":{\"dynamic\":true,\"default\":\"5\",\"validation\":{\"type\":\"integer\",\"min\":\"1\"}},"
                + "\"plugin.flag\":{\"dynamic\":false,\"default\":\"true\",\"validation\":{\"type\":\"boolean\"}},"
                + "\"plugin.with\":{\"dynamic\":true,\"default\":\"5\"},"
                + "\"plugin.without\":{\"dynamic\":false}}}",
            json
        );
    }
}
