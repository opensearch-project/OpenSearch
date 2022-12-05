/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.sampleextension.bwc;

import org.junit.Assert;
import org.opensearch.jobscheduler.sampleextension.SampleExtensionIntegTestCase;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobSchedulerBackwardsCompatibilityIT extends SampleExtensionIntegTestCase {
    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");

    /*
    * In this backward compatibility test, only the old version of job-scheduler plugin is loaded in old cluster while both the latest job-scheduler
    * & sample-extension plugin is loaded in fully upgraded cluster.
     */
    @SuppressWarnings("unchecked")
    public void testBackwardsCompatibility() throws Exception {
        String uri = getPluginUri();
        assert uri != null;

        Map<String, Object> response = getAsMap(uri);
        Map<String, Map<String, Object>> responseMap = (Map<String, Map<String, Object>>) response.get("nodes");

        for (Map<String, Object> respValues: responseMap.values()) {
            List<Map<String, Object>> plugins = (List<Map<String, Object>>) respValues.get("plugins");
            List<String> pluginNames = plugins.stream().map(plugin -> plugin.get("name").toString()).collect(Collectors.toList());

            switch (CLUSTER_TYPE) {
                case OLD:
                case MIXED:
                    /*
                    * as only the old version of job-scheduler plugin is loaded, we only assert that it is loaded.
                     */
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    break;
                case UPGRADED:
                    /*
                    * As cluster is fully upgraded either by full restart or rolling upgrade, we assert, that all nodes are upgraded to use latest plugins.
                    * we trigger a call for scheduling watcher job now.
                     */
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler-sample-extension"));
                    if (CLUSTER_TYPE == ClusterType.UPGRADED || "third".equals(System.getProperty("tests.rest.bwcsuite_round"))) {
                        createBasicWatcherJob();
                    }
            }
        }
    }

    private String getPluginUri() {
        switch (CLUSTER_TYPE) {
            case OLD:
                return "_nodes/" + CLUSTER_NAME + "-0/plugins";
            case MIXED: {
                return getPluginUriForMixedCluster(System.getProperty("tests.rest.bwcsuite_round"));
            }
            case UPGRADED: return "_nodes/plugins";
        }
        return null;
    }

    private String getPluginUriForMixedCluster(String node) {
        switch (node) {
            case "second": return "_nodes/" + CLUSTER_NAME + "-1/plugins";
            case "third": return "_nodes/" + CLUSTER_NAME + "-2/plugins";
            default: return "_nodes/" + CLUSTER_NAME + "-0/plugins";
        }
    }

    private enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;
        static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("Unknown cluster type: $value");
            }
        }
    }

    private void createBasicWatcherJob() throws Exception {
        String index = createTestIndex();
        Instant now = Instant.now();
        /*
        * we insert the oldest version of job metadata directly into the registered index of sample-extension plugin. this will avoid
        * calling the serde methods of ScheduledJobParameter class.
        * Once this doc is inserted, JobSweeper listens for changes in the registered index, tries to deserialize, & then schedule the job.
        * Thus, failure to schedule the job would mean, backward incompatible changes were made in the serde logic.
        * & the assert would fail.
         */
        String jobParameter =
                 "{" +
                    "\"name\":\"sample-job-it\"," +
                    "\"enabled\":true," +
                    "\"enabled_time\":" + now.toEpochMilli() + ", " +
                    "\"last_update_time\":" + now.toEpochMilli() + ", " +
                    "\"schedule\":{" +
                        "\"interval\":{" +
                            "\"start_time\":" + now.toEpochMilli() + "," +
                            "\"period\":1," +
                            "\"unit\":\"Minutes\"" +
                        "}" +
                    "}," +
                    "\"index_name_to_watch\":\"" + index + "\"," +
                    "\"lock_duration_seconds\":120" +
                 "}";

        // Creates a new watcher job.
        String jobId = OpenSearchRestTestCase.randomAlphaOfLength(10);
        createWatcherJobJson(jobId, jobParameter);

        long actualCount = waitAndCountRecords(index, 80000);
        Assert.assertEquals(1, actualCount);
    }
}