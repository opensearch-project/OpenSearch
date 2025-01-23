/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClasspathPluginIT extends OpenSearchIntegTestCase {

    public interface SampleExtension {}

    public static class SampleExtensiblePlugin extends Plugin implements ExtensiblePlugin {
        public SampleExtensiblePlugin() {}

        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            int nLoaded = 0;
            for (SampleExtension e : loader.loadExtensions(SampleExtension.class)) {
                nLoaded++;
            }

            assertThat(nLoaded, equalTo(1));
        }
    }

    public static class SampleExtendingPlugin extends Plugin implements SampleExtension {
        public SampleExtendingPlugin() {}
    };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(SampleExtensiblePlugin.class, SampleExtendingPlugin.class))
            .collect(Collectors.toList());
    }

    @Override
    protected Map<Class<? extends Plugin>, Class<? extends Plugin>> extendedPlugins() {
        return Map.of(SampleExtendingPlugin.class, SampleExtensiblePlugin.class);
    }

    public void testPluginExtensionWithClasspathPlugins() throws IOException {
        internalCluster().startNode();
    }
}
