/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UserPersistenceIT extends HttpSmokeTestCaseWithIdentity {

    public static final String TEST_RESOURCE_RELATIVE_PATH = "../../resources/test/";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = super.nodePlugins().stream().collect(Collectors.toList());
        return plugins;
    }

    public void testUserPersistence() throws Exception {
        final String defaultInitDirectory = new File(TEST_RESOURCE_RELATIVE_PATH + "persistence").getAbsolutePath();
        System.setProperty("identity.default_init.dir", defaultInitDirectory);

        ensureGreen();
    }
}
