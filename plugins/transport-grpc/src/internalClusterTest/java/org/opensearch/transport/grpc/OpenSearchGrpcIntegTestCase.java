/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

public abstract class OpenSearchGrpcIntegTestCase extends OpenSearchIntegTestCase {
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected boolean addMockTransportService() {
        return true;
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.GRPC_EXPERIMENTAL, true).build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return super.nodeSettings(nodeOrdinal);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GrpcModulePlugin.class);
    }
}
