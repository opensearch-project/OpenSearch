/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.NodeHealthService;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class NodeDecommissionService extends AbstractLifecycleComponent implements NodeHealthService {
    private static final Logger logger = LogManager.getLogger(NodeConnectionsService.class);
    private List<String> weights;

    public static final Setting<List<String>> WEIGHTS = Setting.listSetting(
        "cluster.routing.operation_routing.weights",
        emptyList(),
        Function.identity(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    @Inject
    public NodeDecommissionService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, TransportService transportService) {
        this.weights = WEIGHTS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(WEIGHTS, this::setWeights);

    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public StatusInfo getHealth() {
        return null;
    }

    public void setWeights(List<String> weights)
    {
        this.weights = weights;
    }

    public Map<String, Double> getZoneWeightMap(List<String> settingValue){
        Map<String, Double> zoneWeightMap = new HashMap<>();
        for (String val : settingValue) {
            String zone = val.split(":")[0];
            String weight = val.split(":")[1];
            zoneWeightMap.put(zone, Double.parseDouble(weight));
        }
        return zoneWeightMap;
    }

}
