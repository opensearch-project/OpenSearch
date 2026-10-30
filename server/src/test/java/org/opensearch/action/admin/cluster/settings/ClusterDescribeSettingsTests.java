/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingValidation;
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
                SettingMetadata.withDefault(true, "false").withValidation(SettingValidation.ofType("boolean")),
                "plugin.static",
                SettingMetadata.withDefault(false, "false").withValidation(SettingValidation.ofType("boolean")),
                "plugin.remote.analytics.enabled",
                SettingMetadata.withDefault(true, "false").withValidation(SettingValidation.ofType("boolean"))
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
                SettingMetadata.withDefault(true, "1000").withValidation(SettingValidation.of("integer", "1", null)),
                "plugin.size",
                SettingMetadata.withDefault(false, "41943040b").withValidation(SettingValidation.ofType("byte_size")),
                "plugin.interval",
                SettingMetadata.withDefault(false, "30s"),
                "plugin.name",
                SettingMetadata.withDefault(false, "shard-aware")
            ),
            response.settings()
        );
    }

    public void testValidationOfBoundedParsers() {
        assertEquals(
            SettingValidation.of("integer", "1", "10"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.intSetting("p.int", 5, 1, 10, NodeScope))
        );
        assertEquals(
            SettingValidation.of("long", "2", null),
            TransportClusterDescribeSettingsAction.validationOf(Setting.longSetting("p.long", 5L, 2L, NodeScope))
        );
        assertEquals(
            SettingValidation.of("float", "0.5", null),
            TransportClusterDescribeSettingsAction.validationOf(Setting.floatSetting("p.float", 1.5f, 0.5f, NodeScope))
        );
        assertEquals(
            SettingValidation.of("double", "0.5", "2.0"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.doubleSetting("p.double", 1.5, 0.5, 2.0, NodeScope))
        );
        assertEquals(
            SettingValidation.of("byte_size", "1kb", "1gb"),
            TransportClusterDescribeSettingsAction.validationOf(
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
            SettingValidation.of("byte_size", null, "1gb"),
            TransportClusterDescribeSettingsAction.validationOf(
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
            SettingValidation.of("time", "0ms", null),
            TransportClusterDescribeSettingsAction.validationOf(
                Setting.positiveTimeSetting("p.time.min", TimeValue.timeValueSeconds(5), NodeScope)
            )
        );
        assertEquals(
            SettingValidation.of("time", "1s", "1m"),
            TransportClusterDescribeSettingsAction.validationOf(
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
            SettingValidation.of("integer", "0", "100"),
            TransportClusterDescribeSettingsAction.validationOf(affix.getConcreteSettingForNamespace("ns"))
        );
    }

    public void testValidationOfReportsTypeWithoutBoundsWhenSentinelsAreUsed() {
        // The unbounded integer/long factories delegate to the ranged overload with the type's MIN/MAX as sentinels; those
        // admit every value, so the type is reported but the sentinels are not.
        assertEquals(
            SettingValidation.ofType("integer"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.intSetting("p.int", 5, NodeScope))
        );
        assertEquals(
            SettingValidation.ofType("long"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.longSetting("p.long", 5L, NodeScope))
        );
        // Float.MIN_VALUE and Double.MIN_VALUE are the smallest POSITIVE values, so the "unbounded" float/double factories
        // actually reject zero and negatives. That lower bound is real and is reported; only MAX_VALUE is suppressed.
        assertEquals(
            SettingValidation.of("float", Float.toString(Float.MIN_VALUE), null),
            TransportClusterDescribeSettingsAction.validationOf(Setting.floatSetting("p.float", 1.5f, NodeScope))
        );
        assertEquals(
            SettingValidation.of("double", Double.toString(Double.MIN_VALUE), null),
            TransportClusterDescribeSettingsAction.validationOf(Setting.doubleSetting("p.double", 1.5, NodeScope))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.floatSetting("p.float", 1.5f, NodeScope).get(Settings.builder().put("p.float", "0").build())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.doubleSetting("p.double", 1.5, NodeScope).get(Settings.builder().put("p.double", "-1").build())
        );
        assertEquals(
            SettingValidation.ofType("byte_size"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.byteSizeSetting("p.bytes", new ByteSizeValue(1), NodeScope))
        );
        // Upper bounds declared on their own are kept.
        assertEquals(
            SettingValidation.of("long", null, "9"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.longSetting("p.long.max", 5L, Long.MIN_VALUE, 9L, NodeScope))
        );
        // A float declared with Float.MIN_VALUE as its minimum keeps that (positive) minimum alongside the maximum.
        assertEquals(
            SettingValidation.of("float", Float.toString(Float.MIN_VALUE), "9.0"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.floatSetting("p.float.max", 1.5f, Float.MIN_VALUE, 9.0f, NodeScope))
        );
    }

    public void testValidationOfTypeOnlyParsers() {
        // Boolean settings accept only true/false; the type is the whole constraint.
        assertEquals(
            SettingValidation.ofType("boolean"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.boolSetting("p.bool", false, NodeScope))
        );
        assertEquals(
            SettingValidation.ofType("boolean"),
            TransportClusterDescribeSettingsAction.validationOf(
                Setting.boolSetting("p.bool.fallback", Setting.boolSetting("p.bool", false, NodeScope), NodeScope)
            )
        );
        // Memory size accepts an absolute byte size or a percentage of heap; the percentage form is bounded to [0, 100].
        assertEquals(
            SettingValidation.of("memory_size", "0%", "100%"),
            TransportClusterDescribeSettingsAction.validationOf(Setting.memorySizeSetting("p.memory", "50%", NodeScope))
        );
        assertEquals(
            SettingValidation.of("memory_size", "0%", "100%"),
            TransportClusterDescribeSettingsAction.validationOf(
                Setting.memorySizeSetting("p.memory.bytes", new ByteSizeValue(1, ByteSizeUnit.GB), NodeScope)
            )
        );
    }

    public void testValidationOfReturnsNullForOpaqueParsers() {
        // These factories wrap a lambda; neither the type nor any bound can be read from it.
        assertNull(
            TransportClusterDescribeSettingsAction.validationOf(Setting.timeSetting("p.time", TimeValue.timeValueSeconds(1), NodeScope))
        );
        assertNull(TransportClusterDescribeSettingsAction.validationOf(Setting.simpleString("p.string", NodeScope)));
        assertNull(
            TransportClusterDescribeSettingsAction.validationOf(
                new Setting<>("p.enum", "all", s -> s.toUpperCase(java.util.Locale.ROOT), NodeScope)
            )
        );
        assertNull(TransportClusterDescribeSettingsAction.validationOf(SecureSetting.secureString("p.secret", null)));
    }

    public void testEveryBuiltInSettingDescribesWithoutError() throws Exception {
        // Exhaustive guard against null validation or unexpected parser shapes. validationOf runs over every cluster and index
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
            SettingValidation validation = TransportClusterDescribeSettingsAction.validationOf(setting);
            if (validation != null) {
                bounded++;
                assertNotNull(setting.getKey(), validation.type());
                // A sentinel leaking through would surface as one of these strings. "-1" is only a sentinel for byte sizes;
                // time settings legitimately declare -1 ("disabled") as their minimum and must report it.
                // Integer/long MIN_VALUE and every MAX_VALUE mean "unbounded" and must be suppressed. Float/Double MIN_VALUE
                // are positive and therefore real bounds; they are allowed to appear (no built-in setting declares them today).
                for (String sentinel : List.of(
                    Integer.toString(Integer.MIN_VALUE),
                    Integer.toString(Integer.MAX_VALUE),
                    Long.toString(Long.MIN_VALUE),
                    Long.toString(Long.MAX_VALUE),
                    Float.toString(Float.MAX_VALUE),
                    Double.toString(Double.MAX_VALUE)
                )) {
                    assertNotEquals(setting.getKey() + " min", sentinel, validation.min());
                    assertNotEquals(setting.getKey() + " max", sentinel, validation.max());
                }
                if ("byte_size".equals(validation.type())) {
                    assertNotEquals(setting.getKey() + " min", "-1", validation.min());
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
            "--> validationOf over {} built-in settings ({} with declared bounds); full describe path over {}",
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
                SettingMetadata.withDefault(true, "5").withValidation(SettingValidation.of("integer", "1", "10")),
                "plugin.unbounded",
                SettingMetadata.withDefault(false, "5").withValidation(SettingValidation.ofType("integer"))
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
                SettingMetadata.withDefault(true, "5").withValidation(SettingValidation.of("integer", "1", null)),
                "plugin.capped",
                SettingMetadata.withoutDefault(false).withValidation(SettingValidation.of("byte_size", null, "1gb"))
            )
        );
        // The response is local-only: it is rendered with toXContent on the receiving node and never put on the wire.
        expectThrows(UnsupportedOperationException.class, () -> response.writeTo(new BytesStreamOutput()));
        expectThrows(UnsupportedOperationException.class, () -> new ClusterDescribeSettingsResponse((StreamInput) null));
    }

    public void testValueObjectContracts() {
        var validation = SettingValidation.of("integer", "1", "10");
        assertEquals("integer", validation.type());
        assertEquals("1", validation.min());
        assertEquals("10", validation.max());
        assertEquals(validation, SettingValidation.of("integer", "1", "10"));
        assertEquals(validation.hashCode(), SettingValidation.of("integer", "1", "10").hashCode());
        assertNotEquals(validation, SettingValidation.of("integer", "1", null));
        assertNotEquals(validation, SettingValidation.of("long", "1", "10"));
        assertNotEquals(validation, null);
        assertNotEquals(validation, "integer");
        assertEquals("SettingValidation{type=integer, min=1, max=10}", validation.toString());

        var metadata = SettingMetadata.withDefault(true, "5").withValidation(validation);
        assertTrue(metadata.dynamic());
        assertEquals("5", metadata.defaultValue());
        assertEquals(validation, metadata.validation());
        assertNull(SettingMetadata.withoutDefault(false).defaultValue());
        assertNull(SettingMetadata.withoutDefault(false).validation());
        assertEquals(validation, validation);
        assertEquals(metadata, metadata);
        assertEquals(SettingValidation.ofType("boolean"), SettingValidation.of("boolean", null, null));
        assertNull(SettingValidation.ofType("boolean").min());
        assertNull(SettingValidation.ofType("boolean").max());
        assertEquals(metadata, SettingMetadata.withDefault(true, "5").withValidation(SettingValidation.of("integer", "1", "10")));
        assertEquals(metadata.hashCode(), SettingMetadata.withDefault(true, "5").withValidation(validation).hashCode());
        assertNotEquals(metadata, SettingMetadata.withDefault(true, "5"));
        assertNotEquals(metadata, SettingMetadata.withDefault(false, "5").withValidation(validation));
        assertNotEquals(metadata, SettingMetadata.withDefault(true, "6").withValidation(validation));
        assertNotEquals(metadata, null);
        assertNotEquals(metadata, validation);
        assertEquals(
            "SettingMetadata{dynamic=true, default=5, validation=SettingValidation{type=integer, min=1, max=10}}",
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
                SettingMetadata.withDefault(true, "5").withValidation(SettingValidation.of("integer", "1", null)),
                "plugin.flag",
                SettingMetadata.withDefault(false, "true").withValidation(SettingValidation.ofType("boolean"))
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
