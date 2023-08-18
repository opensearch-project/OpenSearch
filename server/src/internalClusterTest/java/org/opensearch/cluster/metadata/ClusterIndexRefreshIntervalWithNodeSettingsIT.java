/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.IndicesService;

public class ClusterIndexRefreshIntervalWithNodeSettingsIT extends ClusterIndexRefreshIntervalIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), getDefaultRefreshInterval())
            .put(
                IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(),
                getMinRefreshIntervalForRefreshDisabled().toString()
            )
            .build();
    }

    @Override
    protected TimeValue getMinRefreshIntervalForRefreshDisabled() {
        return TimeValue.timeValueSeconds(1);
    }

    @Override
    protected TimeValue getDefaultRefreshInterval() {
        return TimeValue.timeValueSeconds(5);
    }
}
