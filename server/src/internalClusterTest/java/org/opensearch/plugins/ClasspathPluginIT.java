/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.Version;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                SampleExtensiblePlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                SampleExtensiblePlugin.class.getName(),
                null,
                Collections.emptyList(),
                false
            ),
            new PluginInfo(
                SampleExtendingPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                SampleExtendingPlugin.class.getName(),
                null,
                List.of(SampleExtensiblePlugin.class.getName()),
                false
            )
        );
    }

    public void testPluginExtensionWithClasspathPlugins() throws IOException {
        internalCluster().startNode();
    }
}
