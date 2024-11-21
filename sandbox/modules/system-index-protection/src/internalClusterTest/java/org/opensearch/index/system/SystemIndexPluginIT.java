/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.system;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.systemindex.SystemIndexProtectionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.tasks.TaskResultsService.TASK_INDEX;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class SystemIndexPluginIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SystemIndexProtectionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SystemIndexProtectionPlugin.SYSTEM_INDEX_PROTECTION_ENABLED_KEY, true)
            .build();
    }

    public void testBasic() throws Exception {
        assertAcked(prepareCreate(TASK_INDEX));
        client().prepareDelete().setIndex(TASK_INDEX);
        assertThrows(OpenSearchSecurityException.class, () -> { admin().indices().prepareDelete(TASK_INDEX).get(); });
    }
}
