/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.junit.Before;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

/**
 * A unit test to validate the former name of the setting 'reindex.remote.allowlist' still take effect, 
 * after it is deprecated, so that the backwards compatibility is maintained.
 * The test can be removed along with removing support of the deprecated setting.
 */
public class ReindexRenamingSettingTests extends OpenSearchTestCase {
    ReindexPlugin plugin;
    
    @Before
    public void setup() {
        this.plugin = new ReindexPlugin();
    }
    
    public void testReindexSettingsExist() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue("Both 'reindex.remote.allowlist' and its predecessor should exist in settings of Reindex plugin",
                settings.containsAll(
                        Arrays.asList(
                                TransportReindexAction.REMOTE_CLUSTER_WHITELIST,
                                TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST
                        )
                )
        );
    }

    public void testSettingFallback() {
        assertEquals(
                TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(Settings.EMPTY),
                TransportReindexAction.REMOTE_CLUSTER_WHITELIST.get(Settings.EMPTY)
        );
    }
    
    public void testSettingGetValue() {
        Settings settings = Settings.builder().put("reindex.remote.allowlist", "127.0.0.1:*").build();
        assertEquals(TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(settings), Arrays.asList("127.0.0.1:*"));
        assertEquals(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.get(settings), Arrays.asList());
    }

    public void testSettingGetValueWithFallback() {
        Settings settings = Settings.builder().put("reindex.remote.whitelist", "127.0.0.1:*").build();
        assertEquals(TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(settings), Arrays.asList("127.0.0.1:*"));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{TransportReindexAction.REMOTE_CLUSTER_WHITELIST});
    }
}
