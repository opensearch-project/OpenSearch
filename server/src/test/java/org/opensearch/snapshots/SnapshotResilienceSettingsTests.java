/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.test.OpenSearchTestCase;

public class SnapshotResilienceSettingsTests extends OpenSearchTestCase {

    public void testRemovedSettingsAreNotRegistered() {
        assertUnknownNodeSetting("snapshot.repository.io_timeout", "10m");
        assertUnknownNodeSetting("snapshot.repository.max_outstanding_ops", "2");
        assertUnknownNodeSetting("snapshot.delete.cleanup_stale_blobs", "false");
    }

    private void assertUnknownNodeSetting(String key, String value) {
        Settings settings = Settings.builder().put(key, value).build();
        SettingsException e = expectThrows(SettingsException.class, () -> new SettingsModule(settings));
        assertTrue(e.getMessage(), e.getMessage().contains("unknown setting [" + key + "]"));
    }
}
