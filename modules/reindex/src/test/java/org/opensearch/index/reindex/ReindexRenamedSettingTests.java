/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

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
public class ReindexRenamedSettingTests extends OpenSearchTestCase {
    private final ReindexPlugin plugin = new ReindexPlugin();

    /**
     * Validate the both settings are known and supported.
     */
    public void testReindexSettingsExist() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue(
            "Both 'reindex.remote.allowlist' and its predecessor should be supported settings of Reindex plugin",
            settings.containsAll(
                Arrays.asList(TransportReindexAction.REMOTE_CLUSTER_WHITELIST, TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST)
            )
        );
    }

    /**
     * Validate the default value of the both settings is the same.
     */
    public void testSettingFallback() {
        assertEquals(
            TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(Settings.EMPTY),
            TransportReindexAction.REMOTE_CLUSTER_WHITELIST.get(Settings.EMPTY)
        );
    }

    /**
     * Validate the new setting can be configured correctly, and it doesn't impact the old setting.
     */
    public void testSettingGetValue() {
        Settings settings = Settings.builder().put("reindex.remote.allowlist", "127.0.0.1:*").build();
        assertEquals(TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(settings), Arrays.asList("127.0.0.1:*"));
        assertEquals(
            TransportReindexAction.REMOTE_CLUSTER_WHITELIST.get(settings),
            TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getDefault(Settings.EMPTY)
        );
    }

    /**
     * Validate the value of the old setting will be applied to the new setting, if the new setting is not configured.
     */
    public void testSettingGetValueWithFallback() {
        Settings settings = Settings.builder().put("reindex.remote.whitelist", "127.0.0.1:*").build();
        assertEquals(TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(settings), Arrays.asList("127.0.0.1:*"));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { TransportReindexAction.REMOTE_CLUSTER_WHITELIST });
    }

    /**
     * Validate the value of the old setting will be ignored, if the new setting is configured.
     */
    public void testSettingGetValueWhenBothAreConfigured() {
        Settings settings = Settings.builder()
            .put("reindex.remote.allowlist", "127.0.0.1:*")
            .put("reindex.remote.whitelist", "[::1]:*, 127.0.0.1:*")
            .build();
        assertEquals(TransportReindexAction.REMOTE_CLUSTER_ALLOWLIST.get(settings), Arrays.asList("127.0.0.1:*"));
        assertEquals(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.get(settings), Arrays.asList("[::1]:*", "127.0.0.1:*"));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { TransportReindexAction.REMOTE_CLUSTER_WHITELIST });
    }

}
