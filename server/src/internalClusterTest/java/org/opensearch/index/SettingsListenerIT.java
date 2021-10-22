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

package org.opensearch.index;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.test.OpenSearchIntegTestCase.Scope.SUITE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
public class SettingsListenerIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SettingsListenerPlugin.class);
    }

    public static class SettingsListenerPlugin extends Plugin {
        private final SettingsTestingService service = new SettingsTestingService();

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(SettingsTestingService.VALUE);
        }

        @Override
        public void onIndexModule(IndexModule module) {
            if (module.getIndex().getName().equals("test")) { // only for the test index
                module.addSettingsUpdateConsumer(SettingsTestingService.VALUE, service::setValue);
                service.setValue(SettingsTestingService.VALUE.get(module.getSettings()));
            }
        }

        @Override
        public Collection<Module> createGuiceModules() {
            return Collections.<Module>singletonList(new SettingsListenerModule(service));
        }
    }

    public static class SettingsListenerModule extends AbstractModule {
        private final SettingsTestingService service;

        public SettingsListenerModule(SettingsTestingService service) {
            this.service = service;
        }

        @Override
        protected void configure() {
            bind(SettingsTestingService.class).toInstance(service);
        }
    }

    public static class SettingsTestingService {
        public volatile int value;
        public static Setting<Integer> VALUE = Setting.intSetting("index.test.new.setting", -1, -1, Property.Dynamic, Property.IndexScope);

        public void setValue(int value) {
            this.value = value;
        }

    }

    public void testListener() {
        assertAcked(
            client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.test.new.setting", 21).build()).get()
        );

        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(21, instance.value);
        }

        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.test.new.setting", 42)).get();
        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("other")
                .setSettings(Settings.builder().put("index.test.new.setting", 21).build())
                .get()
        );

        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        client().admin().indices().prepareUpdateSettings("other").setSettings(Settings.builder().put("index.test.new.setting", 84)).get();

        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        try {
            client().admin()
                .indices()
                .prepareUpdateSettings("other")
                .setSettings(Settings.builder().put("index.test.new.setting", -5))
                .get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [-5] for setting [index.test.new.setting] must be >= -1", ex.getMessage());
        }
    }
}
