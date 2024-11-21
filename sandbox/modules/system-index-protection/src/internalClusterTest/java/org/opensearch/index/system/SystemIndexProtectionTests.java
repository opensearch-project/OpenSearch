/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.system;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.systemindex.SystemIndexProtectionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;

import static org.opensearch.tasks.TaskResultsService.TASK_INDEX;

public class SystemIndexProtectionTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(SystemIndexProtectionPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(SystemIndexProtectionPlugin.SYSTEM_INDEX_PROTECTION_ENABLED_KEY, true)
            .build();
    }

    public void testBasic() throws Exception {
        createIndex(TASK_INDEX);
        DeleteIndexRequestBuilder deleteIndexRequestBuilder = client().admin().indices().prepareDelete(TASK_INDEX);
        assertThrows(OpenSearchSecurityException.class, deleteIndexRequestBuilder::get);
    }

}
