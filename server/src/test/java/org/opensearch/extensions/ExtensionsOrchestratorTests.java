/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.test.OpenSearchTestCase;

public class ExtensionsOrchestratorTests extends OpenSearchTestCase {

    public void testReadFromExtensionsYml() throws Exception {

        ExtensionsSettings extensions = ExtensionsOrchestrator.readFromExtensionsYml("/config/extensions.yml");

        assertNotNull(extensions);
        assertEquals("firstExtension", extensions.getExtensions().get(0).getName());
        assertEquals("uniqueid1", extensions.getExtensions().get(0).getEphemeralId());
        assertEquals("myIndependentPluginHost1", extensions.getExtensions().get(0).getHostName());
        assertEquals("127.0.0.0", extensions.getExtensions().get(0).getHostAddress());
        assertEquals("9301", extensions.getExtensions().get(0).getPort());
        assertEquals("2.0.0", extensions.getExtensions().get(0).getVersion());
        assertEquals("secondExtension", extensions.getExtensions().get(1).getName());
        assertEquals("uniqueid2", extensions.getExtensions().get(1).getEphemeralId());
        assertEquals("myIndependentPluginHost2", extensions.getExtensions().get(1).getHostName());
        assertEquals("127.0.0.0", extensions.getExtensions().get(1).getHostAddress());
        assertEquals("9301", extensions.getExtensions().get(1).getPort());
        assertEquals("2.0.0", extensions.getExtensions().get(1).getVersion());

    }

}
