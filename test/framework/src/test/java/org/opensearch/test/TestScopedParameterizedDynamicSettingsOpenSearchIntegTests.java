/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.hamcrest.CoreMatchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class TestScopedParameterizedDynamicSettingsOpenSearchIntegTests extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {
    public TestScopedParameterizedDynamicSettingsOpenSearchIntegTests(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testSettings() throws IOException {
        final ClusterStateResponse cluster = client().admin().cluster().prepareState().all().get();
        assertThat(
            CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.get(cluster.getState().getMetadata().settings()),
            equalTo(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.get(settings))
        );
    }
}
