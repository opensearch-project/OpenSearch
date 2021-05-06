/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is extension of {@link Throttler} and does throttling of master tasks.
 *
 * This class does throttling on task submission to master, it uses class name of request of tasks as key for
 * throttling. Throttling will be performed over task executor's class level, different task types have different executors class.
 *
 * Set specific setting to for setting the threshold of throttling of particular task type.
 * e.g : Set "master.throttling.thresholds.put_mapping" to set throttling limit of "put mapping" tasks,
 *       Set it to default value(-1) to disable the throttling for this task type.
 */
public class MasterTaskThrottler extends Throttler<String> {
    private static final Logger logger = LogManager.getLogger(MasterTaskThrottler.class);

    public static final Setting<Settings> THRESHOLD_SETTINGS =
            Setting.groupSetting("master.throttling.thresholds.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * To configure more task for throttling, override getMasterThrottlingKey method with task name in task executor.
     * Verify that throttled tasks would be retry.
     *
     * Added retry mechanism in TransportMasterNodeAction so it would be retried for customer generated tasks.
     */
    public static Set<String> CONFIGURED_TASK_FOR_THROTTLING =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "update-settings", "cluster-update-settings",
            "create-index", "auto-create",
            "delete-index", "delete-dangling-index",
            "create-data-stream", "remove-data-stream",
            "rollover-index", "index-aliases",
            "put-mapping", "refresh-mapping",
            "close-indices", "open-indices",
            "create-index-template", "remove-index-template",
            "create-component-template", "remove-component-template",
            "create-index-template-v2", "remove-index-template-v2",
            "put-pipeline", "delete-pipeline",
            "create-persistent-task", "finish-persistent-task", "remove-persistent-task","update-task-state",
            "put-script", "delete-script",
            "put_repository", "delete_repository",
            "create-snapshot","delete-snapshot","update-snapshot-state",
            "restore_snapshot",
            "cluster-reroute-api"
        )));

    private final int DEFAULT_THRESHOLD_VALUE = -1; // Disabled throttling
    private final MasterThrottlingStats throttlingStats = new MasterThrottlingStats();
    private final MasterService masterService;

    public MasterTaskThrottler(final ClusterSettings clusterSettings, final MasterService masterService) {
        super(true);
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTINGS, this::updateSetting, this::validateSetting);
        this.masterService = masterService;
    }

    public void validateSetting(final Settings settings) {
        /**
         * TODO: Change the version number of check as per version in which this change will be merged.
         */
        if(masterService.state().nodes().getMinNodeVersion().compareTo(Version.V_7_10_3) < 0) {
            throw new IllegalArgumentException("All the nodes in cluster should be on version later than 7.10.3");
        }
        Map<String, Settings> groups = settings.getAsGroups();
        for(String key : groups.keySet()) {
            if(!CONFIGURED_TASK_FOR_THROTTLING.contains(key)) {
                throw new IllegalArgumentException("Master task throttling is not configured for given task type: " + key);
            }
            int threshold = groups.get(key).getAsInt("value", DEFAULT_THRESHOLD_VALUE);
            if(threshold < DEFAULT_THRESHOLD_VALUE) {
                throw new IllegalArgumentException("Provide positive integer for limit or -1 for disabling throttling");
            }
        }
    }

    public void updateSetting(final Settings settings) {
        Map<String, Settings> groups = settings.getAsGroups();
        for(String key : groups.keySet()) {
            updateLimit(key, groups.get(key).getAsInt("value", DEFAULT_THRESHOLD_VALUE));
        }
    }

    @Override
    public boolean acquire(final String type, final int permits) {
        boolean ableToAcquire = super.acquire(type, permits);
        if(!ableToAcquire) {
            throttlingStats.incrementThrottlingCount(type, permits);
        }
        return ableToAcquire;
    }

    public MasterThrottlingStats getThrottlingStats() {
        return throttlingStats;
    }

    protected void updateLimit(final String className, final int limit) {
        assert limit >= DEFAULT_THRESHOLD_VALUE;
        if(limit == DEFAULT_THRESHOLD_VALUE) {
            super.removeThrottlingLimit(className);
        } else {
            super.updateThrottlingLimit(className, limit);
        }
    }
}
