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
 *         http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.dashboards;

import org.opensearch.common.settings.Settings;
import org.opensearch.dashboards.action.DeleteSavedObjectAction;
import org.opensearch.dashboards.action.GetAdvancedSettingsAction;
import org.opensearch.dashboards.action.GetSavedObjectAction;
import org.opensearch.dashboards.action.SearchSavedObjectAction;
import org.opensearch.dashboards.action.WriteAdvancedSettingsAction;
import org.opensearch.dashboards.action.WriteSavedObjectAction;
import org.opensearch.dashboards.rest.RestDeleteSavedObjectAction;
import org.opensearch.dashboards.rest.RestGetAdvancedSettingsAction;
import org.opensearch.dashboards.rest.RestGetSavedObjectAction;
import org.opensearch.dashboards.rest.RestSearchSavedObjectAction;
import org.opensearch.dashboards.rest.RestWriteAdvancedSettingsAction;
import org.opensearch.dashboards.rest.RestWriteSavedObjectAction;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class OpenSearchDashboardsModulePluginTests extends OpenSearchTestCase {

    public void testOpenSearchDashboardsIndexNames() {
        assertThat(
            new OpenSearchDashboardsModulePlugin().getSettings(),
            contains(OpenSearchDashboardsModulePlugin.OPENSEARCH_DASHBOARDS_INDEX_NAMES_SETTING)
        );
        assertThat(
            new OpenSearchDashboardsModulePlugin().getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .map(SystemIndexDescriptor::getIndexPattern)
                .collect(Collectors.toList()),
            contains(".opensearch_dashboards", ".opensearch_dashboards_*", ".reporting-*", ".apm-agent-configuration", ".apm-custom-link")
        );
        final List<String> names = Collections.unmodifiableList(Arrays.asList("." + randomAlphaOfLength(4), "." + randomAlphaOfLength(5)));
        final List<String> namesFromDescriptors = new OpenSearchDashboardsModulePlugin().getSystemIndexDescriptors(
            Settings.builder().putList(OpenSearchDashboardsModulePlugin.OPENSEARCH_DASHBOARDS_INDEX_NAMES_SETTING.getKey(), names).build()
        ).stream().map(SystemIndexDescriptor::getIndexPattern).collect(Collectors.toList());
        assertThat(namesFromDescriptors, is(names));

        assertThat(
            new OpenSearchDashboardsModulePlugin().getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .anyMatch(systemIndexDescriptor -> systemIndexDescriptor.matchesIndexPattern(".opensearch_dashboards-event-log-7-1")),
            is(false)
        );
    }

    public void testGetActionsRegistersAdvancedSettingsActions() {
        OpenSearchDashboardsModulePlugin plugin = new OpenSearchDashboardsModulePlugin();
        List<? extends ActionPlugin.ActionHandler<?, ?>> actions = plugin.getActions();

        assertEquals(6, actions.size());

        boolean hasGet = actions.stream().anyMatch(h -> h.getAction().name().equals(GetAdvancedSettingsAction.NAME));
        boolean hasWrite = actions.stream().anyMatch(h -> h.getAction().name().equals(WriteAdvancedSettingsAction.NAME));
        assertTrue(hasGet);
        assertTrue(hasWrite);

        // Saved object actions
        assertTrue(actions.stream().anyMatch(h -> h.getAction().name().equals(GetSavedObjectAction.NAME)));
        assertTrue(actions.stream().anyMatch(h -> h.getAction().name().equals(WriteSavedObjectAction.NAME)));
        assertTrue(actions.stream().anyMatch(h -> h.getAction().name().equals(DeleteSavedObjectAction.NAME)));
        assertTrue(actions.stream().anyMatch(h -> h.getAction().name().equals(SearchSavedObjectAction.NAME)));
    }

    public void testGetRestHandlersIncludesAdvancedSettingsHandlers() {
        OpenSearchDashboardsModulePlugin plugin = new OpenSearchDashboardsModulePlugin();
        List<RestHandler> handlers = plugin.getRestHandlers(Settings.EMPTY, null, null, null, null, null, null);

        List<String> handlerNames = handlers.stream()
            .filter(h -> h instanceof RestGetAdvancedSettingsAction || h instanceof RestWriteAdvancedSettingsAction)
            .map(h -> {
                if (h instanceof RestGetAdvancedSettingsAction) return "get";
                return "write";
            })
            .collect(Collectors.toList());

        assertTrue(handlerNames.contains("get"));
        assertTrue(handlerNames.contains("write"));

        // Saved object REST handlers
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestGetSavedObjectAction));
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestWriteSavedObjectAction));
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestDeleteSavedObjectAction));
        assertTrue(handlers.stream().anyMatch(h -> h instanceof RestSearchSavedObjectAction));
    }
}
