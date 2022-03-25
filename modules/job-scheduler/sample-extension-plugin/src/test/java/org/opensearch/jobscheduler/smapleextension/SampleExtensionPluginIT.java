/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.smapleextension;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.util.List;

public class SampleExtensionPluginIT extends OpenSearchIntegTestCase {

    public void testPluginsAreInstalled() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = OpenSearchIntegTestCase.client().admin().cluster().health(request).actionGet();
        Assert.assertEquals(ClusterHealthStatus.GREEN, response.getStatus());

        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest)
                .actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes().get(0).getInfo(PluginsAndModules.class).getPluginInfos();
        Assert.assertTrue(pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName()
                .equals("opensearch-job-scheduler")));
        Assert.assertTrue(pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName()
                .equals("opensearch-job-scheduler-sample-extension")));
    }
}
