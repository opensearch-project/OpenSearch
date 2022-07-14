/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.hamcrest.Matchers;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class ClusterServiceTests extends OpenSearchTestCase {
    public void testDeprecatedGetMasterServiceWhenUsingMasterServiceToInitializeClusterService() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        MasterService masterService = new MasterService(Settings.EMPTY, clusterSettings, null);
        ClusterService clusterServiceWithMasterService = new ClusterService(Settings.EMPTY, clusterSettings, masterService, null);
        assertThat(clusterServiceWithMasterService.getMasterService(), Matchers.equalTo(masterService));
    }

    public void testDeprecatedGetMasterServiceWithoutAssigningMasterService() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, null);
        assertThat(clusterService.getMasterService(), Matchers.instanceOf(MasterService.class));
    }
}
