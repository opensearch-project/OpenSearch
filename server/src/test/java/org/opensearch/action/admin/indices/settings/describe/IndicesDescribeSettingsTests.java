/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.settings.describe;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.SettingMetadata;
import org.opensearch.common.settings.SettingValidation;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsDescriber;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.Filtered;
import static org.opensearch.common.settings.Setting.Property.IndexScope;
import static org.opensearch.common.settings.Setting.Property.InternalIndex;
import static org.opensearch.common.settings.Setting.Property.PrivateIndex;

public class IndicesDescribeSettingsTests extends OpenSearchTestCase {
    private static final SettingsFilter NO_FILTER = new SettingsFilter(List.of());

    private static IndicesDescribeSettingsResponse describe(IndexScopedSettings registry, SettingsFilter filter, String... names) {
        return new IndicesDescribeSettingsResponse(SettingsDescriber.describe(names, registry, filter));
    }

    public void testBuiltInIndexSettingsReportTheirDefinitions() {
        var response = describe(
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            NO_FILTER,
            "index.number_of_shards",
            "index.number_of_replicas",
            "index.refresh_interval",
            "index.unknown"
        );
        // number_of_shards reads its default from the opensearch.index.number_of_shards system property at class
        // initialisation, so the expectation is taken from the setting rather than hard-coded.
        String declaredShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getDefaultRaw(Settings.EMPTY);
        assertEquals(
            Map.of(
                "index.number_of_shards",
                SettingMetadata.withDefault(false, declaredShards).withValidation(SettingValidation.of("integer", "1", "1024")),
                "index.number_of_replicas",
                SettingMetadata.withDefault(true, "1").withValidation(SettingValidation.of("integer", "0", null)),
                // Declares -1 ("disabled") as its minimum, so unlike a plain timeSetting it reports a time type and bound.
                "index.refresh_interval",
                SettingMetadata.withDefault(true, "1s").withValidation(SettingValidation.of("time", "-1", null))
            ),
            response.settings()
        );
    }

    public void testFilteredSettingsAreOmittedAndUpdateGatesAreNot() {
        var dynamic = Setting.boolSetting("index.test.dynamic", false, Dynamic, IndexScope);
        var fixed = Setting.boolSetting("index.test.static", false, IndexScope);
        var secret = Setting.simpleString("index.test.secret", IndexScope, Filtered);
        var hidden = Setting.simpleString("index.test.hidden", IndexScope);
        // InternalIndex and PrivateIndex reject user updates; they are not visibility controls and GET /_settings?include_defaults
        // lists them, so describe reports them like any other setting.
        var internal = Setting.intSetting("index.test.internal", 1, 0, IndexScope, InternalIndex);
        var privateOne = Setting.simpleString("index.test.private", IndexScope, PrivateIndex);
        var affix = Setting.affixKeySetting("index.test.remote.", "enabled", key -> Setting.boolSetting(key, false, Dynamic, IndexScope));
        var registry = new IndexScopedSettings(Settings.EMPTY, Set.of(dynamic, fixed, secret, hidden, internal, privateOne, affix));
        var filter = new SettingsFilter(List.of("index.test.hidden"));

        var exact = describe(
            registry,
            filter,
            "index.test.dynamic",
            "index.test.static",
            "index.test.secret",
            "index.test.hidden",
            "index.test.internal",
            "index.test.private",
            "index.test.remote.alpha.enabled",
            "index.test.unknown"
        );
        var expected = Map.of(
            "index.test.dynamic",
            SettingMetadata.withDefault(true, "false").withValidation(SettingValidation.ofType("boolean")),
            "index.test.static",
            SettingMetadata.withDefault(false, "false").withValidation(SettingValidation.ofType("boolean")),
            "index.test.internal",
            SettingMetadata.withDefault(false, "1").withValidation(SettingValidation.of("integer", "0", null)),
            "index.test.private",
            SettingMetadata.withoutDefault(false),
            "index.test.remote.alpha.enabled",
            SettingMetadata.withDefault(true, "false").withValidation(SettingValidation.ofType("boolean"))
        );
        assertEquals(expected, exact.settings());

        // A prefix expands to the same visible set minus the concrete affix key, which is not a registered setting.
        var expanded = describe(registry, filter, "index.test.*");
        var expectedExpanded = new java.util.HashMap<>(expected);
        expectedExpanded.remove("index.test.remote.alpha.enabled");
        assertEquals(expectedExpanded, expanded.settings());
        assertEquals(
            List.of("index.test.dynamic", "index.test.internal", "index.test.private", "index.test.static"),
            List.copyOf(expanded.settings().keySet())
        );
        assertEquals(Map.of(), describe(registry, filter, "index.other.*").settings());
    }

    public void testEveryBuiltInIndexSettingDescribesWithoutError() {
        // Exhaustive guard for the index registry: the full describe/render path, including getDefaultRaw(Settings.EMPTY), over
        // every index setting core registers. Affix templates are skipped: describe() only sees concrete keys.
        int described = 0;
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            if (setting instanceof Setting.AffixSetting) {
                continue;
            }
            var response = describe(IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, NO_FILTER, setting.getKey());
            assertEquals(setting.getKey(), 1, response.settings().size());
            assertNotNull(Strings.toString(MediaTypeRegistry.JSON, response));
            described++;
        }
        // The same settings through one prefix expansion, which is how a client discovers them. Group settings (index.analysis.,
        // index.similarity.) have open membership and no concrete key of their own, so a prefix does not list them. The loop above
        // reached them by calling the describer directly with their trailing-dot key; through REST that key fails validateNames
        // (the key grammar rejects a trailing dot), so the two groups are not describable through the API at all. Same rule as
        // the cluster registry.
        var all = describe(IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, NO_FILTER, "index.*");
        var notExpanded = new java.util.TreeSet<String>();
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            if (setting instanceof Setting.AffixSetting == false && all.settings().containsKey(setting.getKey()) == false) {
                notExpanded.add(setting.getKey());
            }
        }
        assertEquals(Set.of("index.analysis.", "index.similarity."), notExpanded);
        assertEquals(described - notExpanded.size(), all.settings().size());
        assertTrue(all.settings().containsKey(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()));
        logger.info("--> full describe path over {} built-in index settings", described);
    }

    public void testRequestValidationAndSerialization() throws Exception {
        assertNotNull(new IndicesDescribeSettingsRequest().validate());
        for (String name : List.of("", " ", "*", "index.*.enabled", "index.?", "index.**", "has space", "index.analysis.")) {
            assertNotNull("expected rejection of [" + name + "]", new IndicesDescribeSettingsRequest(name).validate());
        }
        // The wildcard-shape message quotes an example from this registry, not the cluster one.
        assertTrue(
            new IndicesDescribeSettingsRequest("index.*.enabled").validate().getMessage().contains("such as index.routing.allocation.*")
        );
        assertNull(new IndicesDescribeSettingsRequest("index.number_of_shards", "index.routing.allocation.*").validate());

        var request = new IndicesDescribeSettingsRequest("index.number_of_shards", "index.routing.*");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertArrayEquals(request.names(), new IndicesDescribeSettingsRequest(in).names());
            }
        }
    }

    public void testResponseIsLocalOnlyAndRendersSortedKeys() throws Exception {
        var response = new IndicesDescribeSettingsResponse(
            Map.of(
                "index.z",
                SettingMetadata.withoutDefault(false),
                "index.a",
                SettingMetadata.withDefault(true, "1").withValidation(SettingValidation.of("integer", "0", null))
            )
        );
        assertEquals(
            "{\"settings\":{"
                + "\"index.a\":{\"dynamic\":true,\"default\":\"1\",\"validation\":{\"type\":\"integer\",\"min\":\"0\"}},"
                + "\"index.z\":{\"dynamic\":false}}}",
            Strings.toString(MediaTypeRegistry.JSON, response)
        );
        expectThrows(UnsupportedOperationException.class, () -> response.writeTo(new BytesStreamOutput()));
        try (StreamInput in = new BytesStreamOutput().bytes().streamInput()) {
            expectThrows(UnsupportedOperationException.class, () -> new IndicesDescribeSettingsResponse(in));
        }
        expectThrows(UnsupportedOperationException.class, () -> response.settings().put("x", SettingMetadata.withoutDefault(false)));
    }
}
