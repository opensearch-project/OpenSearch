/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;

/**
 * Remote upload back pressure service.
 *
 * @opensearch.internal
 */
public class RemoteUploadPressureService {

    private static final Logger logger = LogManager.getLogger(RemoteUploadPressureService.class);

    private final RemoteUploadPressureSettings remoteUploadPressureSettings;

    private final RemoteUploadStatsTracker remoteUploadStatsTracker;

    @Inject
    public RemoteUploadPressureService(ClusterService clusterService, Settings settings) {
        remoteUploadStatsTracker = new RemoteUploadStatsTracker();
        remoteUploadPressureSettings = new RemoteUploadPressureSettings(clusterService, settings);
    }
}
