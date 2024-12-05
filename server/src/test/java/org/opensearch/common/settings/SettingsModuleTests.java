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

package org.opensearch.common.settings;

import org.opensearch.common.inject.ModuleTestCase;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.SearchService;
import org.opensearch.test.FeatureFlagSetter;
import org.hamcrest.Matchers;

import java.util.Arrays;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SettingsModuleTests extends ModuleTestCase {

    public void testValidate() {
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "2.0").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]").build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new SettingsModule(settings));
            assertEquals("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
        }

        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]").put("some.foo.bar", 1).build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new SettingsModule(settings));
            assertEquals("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
            assertEquals(1, ex.getSuppressed().length);
            assertEquals(
                "unknown setting [some.foo.bar] please check that any required plugins are installed, or check the breaking "
                    + "changes documentation for removed settings",
                ex.getSuppressed()[0].getMessage()
            );
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default").put("index.foo.bar", 1).build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new SettingsModule(settings));
            assertEquals("node settings must not contain any index level settings", ex.getMessage());
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
    }

    public void testRegisterSettings() {
        {
            Settings settings = Settings.builder().put("some.custom.setting", "2.0").build();
            SettingsModule module = new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("some.custom.setting", "false").build();
            try {
                new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("Failed to parse value [false] for setting [some.custom.setting]", ex.getMessage());
            }
        }
    }

    public void testRegisterConsistentSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("some.custom.secure.consistent.setting", "secure_value");
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Setting<?> concreteConsistentSetting = SecureSetting.secureString(
            "some.custom.secure.consistent.setting",
            null,
            Setting.Property.Consistent
        );
        SettingsModule module = new SettingsModule(settings, concreteConsistentSetting);
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        assertThat(module.getConsistentSettings(), Matchers.containsInAnyOrder(concreteConsistentSetting));

        final Setting<?> concreteUnsecureConsistentSetting = Setting.simpleString(
            "some.custom.UNSECURE.consistent.setting",
            Property.Consistent,
            Property.NodeScope
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SettingsModule(Settings.builder().build(), concreteUnsecureConsistentSetting)
        );
        assertThat(e.getMessage(), is("Invalid consistent secure setting [some.custom.UNSECURE.consistent.setting]"));

        secureSettings = new MockSecureSettings();
        secureSettings.setString("some.custom.secure.consistent.afix.wow.setting", "secure_value");
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings).build();
        final Setting<?> afixConcreteConsistentSetting = Setting.affixKeySetting(
            "some.custom.secure.consistent.afix.",
            "setting",
            key -> SecureSetting.secureString(key, null, Setting.Property.Consistent)
        );
        module = new SettingsModule(settings2, afixConcreteConsistentSetting);
        assertInstanceBinding(module, Settings.class, (s) -> s == settings2);
        assertThat(module.getConsistentSettings(), Matchers.containsInAnyOrder(afixConcreteConsistentSetting));

        final Setting<?> concreteUnsecureConsistentAfixSetting = Setting.affixKeySetting(
            "some.custom.secure.consistent.afix.",
            "setting",
            key -> Setting.simpleString(key, Setting.Property.Consistent, Property.NodeScope)
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SettingsModule(Settings.builder().build(), concreteUnsecureConsistentAfixSetting)
        );
        assertThat(e.getMessage(), is("Invalid consistent secure setting [some.custom.secure.consistent.afix.*.setting]"));
    }

    public void testLoggerSettings() {
        {
            Settings settings = Settings.builder().put("logger._root", "TRACE").put("logger.transport", "INFO").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }

        {
            Settings settings = Settings.builder().put("logger._root", "BOOM").put("logger.transport", "WOW").build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new SettingsModule(settings));
            assertEquals("Unknown level constant [BOOM].", ex.getMessage());
        }
    }

    public void testRegisterSettingsFilter() {
        Settings settings = Settings.builder().put("foo.bar", "false").put("bar.foo", false).put("bar.baz", false).build();
        try {
            new SettingsModule(
                settings,
                Arrays.asList(
                    Setting.boolSetting("foo.bar", true, Property.NodeScope),
                    Setting.boolSetting("bar.foo", true, Property.NodeScope, Property.Filtered),
                    Setting.boolSetting("bar.baz", true, Property.NodeScope)
                ),
                Arrays.asList("foo.*", "bar.foo"),
                emptySet()
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("filter [bar.foo] has already been registered", ex.getMessage());
        }
        SettingsModule module = new SettingsModule(
            settings,
            Arrays.asList(
                Setting.boolSetting("foo.bar", true, Property.NodeScope),
                Setting.boolSetting("bar.foo", true, Property.NodeScope, Property.Filtered),
                Setting.boolSetting("bar.baz", true, Property.NodeScope)
            ),
            Arrays.asList("foo.*"),
            emptySet()
        );
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).size() == 1);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).keySet().contains("bar.baz"));
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).get("bar.baz").equals("false"));

    }

    public void testMutuallyExclusiveScopes() {
        new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar", Property.NodeScope));
        new SettingsModule(Settings.EMPTY, Setting.simpleString("index.foo.bar", Property.IndexScope));

        // Those should fail
        try {
            new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar"));
            fail("No scope should fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("No scope found for setting"));
        }
        // Some settings have both scopes - that's fine too if they have per-node defaults
        try {
            new SettingsModule(
                Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.NodeScope)
            );
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }

        try {
            new SettingsModule(
                Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.IndexScope)
            );
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }
    }

    public void testOldMaxClauseCountSetting() {
        Settings settings = Settings.builder().put("index.query.bool.max_clause_count", 1024).build();
        SettingsException ex = expectThrows(SettingsException.class, () -> new SettingsModule(settings));
        assertEquals(
            "unknown setting [index.query.bool.max_clause_count] did you mean [indices.query.bool.max_clause_count]?",
            ex.getMessage()
        );
    }

    public void testDynamicNodeSettingsRegistration() {
        FeatureFlagSetter.set(FeatureFlags.EXTENSIONS);
        Settings settings = Settings.builder().put("some.custom.setting", "2.0").build();
        SettingsModule module = new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
        assertNotNull(module.getClusterSettings().get("some.custom.setting"));
        // For unregistered setting the value is expected to be null
        assertNull(module.getClusterSettings().get("some.custom.setting2"));
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);

        assertTrue(module.registerDynamicSetting(Setting.floatSetting("some.custom.setting2", 1.0f, Property.NodeScope)));
        assertNotNull(module.getClusterSettings().get("some.custom.setting2"));
        // verify if some.custom.setting still exists
        assertNotNull(module.getClusterSettings().get("some.custom.setting"));

        // verify exception is thrown when setting registration fails
        expectThrows(
            SettingsException.class,
            () -> module.registerDynamicSetting(Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope))
        );
    }

    public void testDynamicIndexSettingsRegistration() {
        FeatureFlagSetter.set(FeatureFlags.EXTENSIONS);
        Settings settings = Settings.builder().put("some.custom.setting", "2.0").build();
        SettingsModule module = new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
        assertNotNull(module.getClusterSettings().get("some.custom.setting"));
        // For unregistered setting the value is expected to be null
        assertNull(module.getIndexScopedSettings().get("index.custom.setting2"));
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);

        assertTrue(module.registerDynamicSetting(Setting.floatSetting("index.custom.setting2", 1.0f, Property.IndexScope)));
        assertNotNull(module.getIndexScopedSettings().get("index.custom.setting2"));

        // verify if some.custom.setting still exists
        assertNotNull(module.getClusterSettings().get("some.custom.setting"));

        // verify exception is thrown when setting registration fails
        expectThrows(
            SettingsException.class,
            () -> module.registerDynamicSetting(Setting.floatSetting("index.custom.setting2", 1.0f, Property.IndexScope))
        );
    }

    public void testConcurrentSegmentSearchClusterSettings() {
        boolean settingValue = randomBoolean();
        Settings settings = Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), settingValue).build();
        SettingsModule settingsModule = new SettingsModule(settings);
        assertEquals(settingValue, SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.get(settingsModule.getSettings()));
        assertSettingDeprecationsAndWarnings(new Setting[] { SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING });
    }

    public void testConcurrentSegmentSearchIndexSettings() {
        Settings.Builder target = Settings.builder().put(Settings.EMPTY);
        Settings.Builder update = Settings.builder();
        boolean settingValue = randomBoolean();
        SettingsModule module = new SettingsModule(Settings.EMPTY);
        IndexScopedSettings indexScopedSettings = module.getIndexScopedSettings();
        indexScopedSettings.updateDynamicSettings(
            Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), settingValue).build(),
            target,
            update,
            "node"
        );
        // apply the setting update
        module.getIndexScopedSettings()
            .applySettings(Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), settingValue).build());
        // assert value
        assertEquals(settingValue, module.getIndexScopedSettings().get(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING));
        assertSettingDeprecationsAndWarnings(new Setting[] { IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING });
    }

    public void testMaxSliceCountClusterSettingsForConcurrentSearch() {
        int settingValue = randomIntBetween(0, 10);
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), settingValue)
            .build();
        SettingsModule settingsModule = new SettingsModule(settings);
        assertEquals(
            settingValue,
            (int) SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.get(settingsModule.getSettings())
        );

        // Test that negative value is not allowed
        settingValue = -1;
        final Settings settings_2 = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), settingValue)
            .build();
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> new SettingsModule(settings_2));
        assertTrue(iae.getMessage().contains(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey()));
    }
}
