/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge;

import org.opensearch.common.settings.Setting;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class NativeBridgeModuleTests extends OpenSearchTestCase {

    public void testGetSettingsReturnsBothDecaySettings() {
        NativeBridgeModule module = new NativeBridgeModule();
        List<Setting<?>> settings = module.getSettings();
        assertEquals(2, settings.size());
        assertEquals("native.jemalloc.dirty_decay_ms", settings.get(0).getKey());
        assertEquals("native.jemalloc.muzzy_decay_ms", settings.get(1).getKey());
    }
}
