/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.accesscontrol.resources.testplugins.TestRACPlugin;
import org.opensearch.accesscontrol.resources.testplugins.TestResourcePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ResourceAccessControlPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;

public class NonDefaultResourceAccessControlPluginIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestResourcePlugin.class, TestRACPlugin.class);
    }

    public void testSampleResourcePluginCallsTestRACPluginToManageResourceAccess() {
        ResourceAccessControlPlugin racPlugin = TestResourcePlugin.GuiceHolder.getResourceService().getResourceAccessControlPlugin();
        MatcherAssert.assertThat(racPlugin.getClass(), Matchers.is(TestRACPlugin.class));
    }
}
