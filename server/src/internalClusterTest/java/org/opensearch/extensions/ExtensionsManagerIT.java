/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ExtensionsManagerIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.EXTENSIONS, "true").build();
    }

    public void testExtensionsManagerCreation() {
        String nodeName = internalCluster().startNode();

        ensureGreen();

        ExtensionsManager extManager = internalCluster().getInstance(ExtensionsManager.class, nodeName);

        assertEquals(ExtensionsManager.class.getName(), extManager.getClass().getName());
    }
}
