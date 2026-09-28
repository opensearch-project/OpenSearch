/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;

public class PluginAdditionalHealthPathsTests extends OpenSearchTestCase {

    public void testDefaultIsEmpty() {
        Plugin plugin = new Plugin() {
        };
        assertEquals(List.of(), plugin.getAdditionalHealthPaths(Settings.EMPTY));
    }

    public void testOverrideReturnsConfiguredPath() {
        Path expected = createTempDir();
        Plugin plugin = new Plugin() {
            @Override
            public List<Path> getAdditionalHealthPaths(Settings settings) {
                return List.of(expected);
            }
        };
        assertEquals(List.of(expected), plugin.getAdditionalHealthPaths(Settings.EMPTY));
    }
}
