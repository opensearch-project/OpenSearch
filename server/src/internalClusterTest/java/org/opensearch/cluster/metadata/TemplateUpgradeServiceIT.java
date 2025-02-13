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

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class TemplateUpgradeServiceIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    public static final class TestPlugin extends Plugin {
        // This setting is used to simulate cluster state updates
        static final Setting<Integer> UPDATE_TEMPLATE_DUMMY_SETTING = Setting.intSetting(
            "tests.update_template_count",
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        private static final Logger logger = LogManager.getLogger(TestPlugin.class);

        protected final Settings settings;

        public TestPlugin(Settings settings) {
            this.settings = settings;
        }

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver expressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(UPDATE_TEMPLATE_DUMMY_SETTING, integer -> {
                logger.debug("the template dummy setting was updated to {}", integer);
            });
            return super.createComponents(
                client,
                clusterService,
                threadPool,
                resourceWatcherService,
                scriptService,
                xContentRegistry,
                environment,
                nodeEnvironment,
                namedWriteableRegistry,
                expressionResolver,
                repositoriesServiceSupplier
            );
        }

        @Override
        public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
            return templates -> {
                templates.put(
                    "test_added_template",
                    IndexTemplateMetadata.builder("test_added_template").patterns(Collections.singletonList("*")).build()
                );
                templates.remove("test_removed_template");
                templates.put(
                    "test_changed_template",
                    IndexTemplateMetadata.builder("test_changed_template").order(10).patterns(Collections.singletonList("*")).build()
                );
                return templates;
            };
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(UPDATE_TEMPLATE_DUMMY_SETTING);
        }
    }

    public void testTemplateUpdate() throws Exception {
        assertTemplates();

        // Change some templates
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("test_dummy_template")
                .setOrder(0)
                .setPatterns(Collections.singletonList("*"))
                .get()
        );
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("test_changed_template")
                .setOrder(0)
                .setPatterns(Collections.singletonList("*"))
                .get()
        );
        assertAcked(
            client().admin()
                .indices()
                .preparePutTemplate("test_removed_template")
                .setOrder(1)
                .setPatterns(Collections.singletonList("*"))
                .get()
        );

        AtomicInteger updateCount = new AtomicInteger();
        // Wait for the templates to be updated back to normal
        assertBusy(() -> {
            // the updates only happen on cluster state updates, so we need to make sure that the cluster state updates are happening
            // so we need to simulate updates to make sure the template upgrade kicks in
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder().put(TestPlugin.UPDATE_TEMPLATE_DUMMY_SETTING.getKey(), updateCount.incrementAndGet())
                    )
                    .get()
            );
            List<IndexTemplateMetadata> templates = client().admin().indices().prepareGetTemplates("test_*").get().getIndexTemplates();
            assertThat(templates, hasSize(3));
            boolean addedFound = false;
            boolean changedFound = false;
            boolean dummyFound = false;
            for (int i = 0; i < 3; i++) {
                IndexTemplateMetadata templateMetadata = templates.get(i);
                switch (templateMetadata.getName()) {
                    case "test_added_template":
                        assertFalse(addedFound);
                        addedFound = true;
                        break;
                    case "test_changed_template":
                        assertFalse(changedFound);
                        changedFound = true;
                        assertThat(templateMetadata.getOrder(), equalTo(10));
                        break;
                    case "test_dummy_template":
                        assertFalse(dummyFound);
                        dummyFound = true;
                        break;
                    default:
                        fail("unexpected template " + templateMetadata.getName());
                        break;
                }
            }
            assertTrue(addedFound);
            assertTrue(changedFound);
            assertTrue(dummyFound);
        });

        // Wipe out all templates
        assertAcked(client().admin().indices().prepareDeleteTemplate("test_*").get());

        assertTemplates();

    }

    private void assertTemplates() throws Exception {
        AtomicInteger updateCount = new AtomicInteger();
        // Make sure all templates are recreated correctly
        assertBusy(() -> {
            // the updates only happen on cluster state updates, so we need to make sure that the cluster state updates are happening
            // so we need to simulate updates to make sure the template upgrade kicks in
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(
                        Settings.builder().put(TestPlugin.UPDATE_TEMPLATE_DUMMY_SETTING.getKey(), updateCount.incrementAndGet())
                    )
                    .get()
            );

            List<IndexTemplateMetadata> templates = client().admin().indices().prepareGetTemplates("test_*").get().getIndexTemplates();
            assertThat(templates, hasSize(2));
            boolean addedFound = false;
            boolean changedFound = false;
            for (int i = 0; i < 2; i++) {
                IndexTemplateMetadata templateMetadata = templates.get(i);
                switch (templateMetadata.getName()) {
                    case "test_added_template":
                        assertFalse(addedFound);
                        addedFound = true;
                        break;
                    case "test_changed_template":
                        assertFalse(changedFound);
                        changedFound = true;
                        assertThat(templateMetadata.getOrder(), equalTo(10));
                        break;
                    default:
                        fail("unexpected template " + templateMetadata.getName());
                        break;
                }
            }

            assertTrue(addedFound);
            assertTrue(changedFound);
        });
    }

}
