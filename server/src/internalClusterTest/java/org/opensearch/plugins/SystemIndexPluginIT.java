/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SystemIndexPluginIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SystemIndexPlugin1.class, SystemIndexPlugin2.class);
    }

    public void test2SystemIndexPluginsImplementOnSystemIndices_shouldFail() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> internalCluster().startNode());
        assertThat(e.getMessage(), containsString("Cannot have more than one plugin implementing onSystemIndex"));
    }

}

final class SystemIndexPlugin1 extends Plugin implements SystemIndexPlugin {

    private Map<String, Set<String>> systemIndices;

    public SystemIndexPlugin1() {}

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        final SystemIndexDescriptor systemIndexDescriptor = new SystemIndexDescriptor(".system-index1", "System index 1");
        return Collections.singletonList(systemIndexDescriptor);
    }

    @Override
    public Consumer<Map<String, Set<String>>> onSystemIndices() {
        return (systemIndices) -> { this.systemIndices = systemIndices; };
    }
}

class SystemIndexPlugin2 extends Plugin implements SystemIndexPlugin {

    private Map<String, Set<String>> systemIndices;

    public SystemIndexPlugin2() {}

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        final SystemIndexDescriptor systemIndexDescriptor = new SystemIndexDescriptor(".system-index2", "System index 2");
        return Collections.singletonList(systemIndexDescriptor);
    }

    @Override
    public Consumer<Map<String, Set<String>>> onSystemIndices() {
        return (systemIndices) -> { this.systemIndices = systemIndices; };
    }
}
