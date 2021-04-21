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
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.dangling.delete.DeleteDanglingIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.opensearch.action.support.master.MasterNodeRequest;
import org.opensearch.cluster.ack.AckedRequest;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.snapshots.UpdateIndexShardSnapshotStatusRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is extension of {@link Throttler} and does throttling of master tasks.
 *
 * This class does throttling on task submission to master, it uses class name of request of tasks as key for
 * throttling. Throttling will be performed over task class level, different task types have different request class, i.e.
 * Throttling is at the task type level.
 *
 * Set "master.throttling.enable" setting to enable/disable this throttling. (Default False)
 * Set specific setting to for setting the threshold of throttling of particular task type.
 * e.g : Set "master.throttling.thresholds.put_mapping" to set throttling limit of "put mapping" tasks,
 *       Set it to default value(-1) to disable the throttling for this task type.
 *
 *       We can also provide the class path of request, which we want to throttle. If new task type has introduced and we
 *       haven't added that in configuration then we can use the class path of request to enable throttling on that type.
 *       e.g use "org/opensearch/action/admin/indices/create/CreateIndexClusterStateUpdateRequest" as key in setting
 *       throttling limit for Create index tasks.
 */
public class MasterTaskThrottler extends Throttler<Class> {
    private static final Logger logger = LogManager.getLogger(MasterTaskThrottler.class);

    public static final Setting<Boolean> ENABLE_MASTER_THROTTLING =
            Setting.boolSetting("master.throttling.enable", false,
                    Setting.Property.Dynamic, Setting.Property.NodeScope);

    public static final Setting<Settings> THRESHOLD_SETTINGS =
            Setting.groupSetting("master.throttling.thresholds.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    protected final ConcurrentHashMap<String, Class> taskNameToClassMapping = new ConcurrentHashMap() {
        {
            put("put_mapping", PutMappingClusterStateUpdateRequest.class);
            put("create_index", CreateIndexRequest.class);
            put("delete_dangling_index", DeleteDanglingIndexRequest.class);
            put("update_snapshot", UpdateIndexShardSnapshotStatusRequest.class);
            put("delete_snapshot", DeleteSnapshotRequest.class);
        }
    };
    private final int DEFAULT_THRESHOLD_VALUE = -1; // Disabled throttling
    private final MasterThrottlingStats throttlingStats = new MasterThrottlingStats();
    private final MasterService masterService;

    public MasterTaskThrottler(final ClusterSettings clusterSettings, final MasterService masterService) {
        super(false);
        clusterSettings.addSettingsUpdateConsumer(ENABLE_MASTER_THROTTLING, this::setMasterThrottlingEnable, this::validateEnableSetting);
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTINGS, this::updateSetting, this::validateSetting);
        this.masterService = masterService;
    }

    private void setMasterThrottlingEnable(boolean enableMasterThrottling) {
        super.setThrottlingEnabled(enableMasterThrottling);
    }

    public void validateEnableSetting(final boolean enableMasterThrottling) {
        if(masterService.state().nodes().getMinNodeVersion().compareTo(Version.V_7_10_3) < 0) {
            throw new IllegalArgumentException("All the nodes in cluster should be on version later than 7.10.3");
        }
    }

    public void validateSetting(final Settings settings) {
        Map<String, Settings> groups = settings.getAsGroups();
        for(String key : groups.keySet()) {
            int threshold = groups.get(key).getAsInt("value", DEFAULT_THRESHOLD_VALUE);
            if(threshold < DEFAULT_THRESHOLD_VALUE) {
                throw new IllegalArgumentException("Provide positive integer for limit or -1 for disabling throttling");
            }
            if(!taskNameToClassMapping.containsKey(key)) {
                try {
                    Class requestClass = Class.forName(key.replace("/", "."));
                    // Below check is to verify if class is of any request to master node or not.
                    // MasterNodeRequest and Cluster State update requests are the requests coming to master.
                    if(!(AckedRequest.class.isAssignableFrom(requestClass) || MasterNodeRequest.class.isAssignableFrom(requestClass))) {
                        throw new IllegalArgumentException("Only Master Node request and Cluster state update request are allowed.");
                    }
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Provide valid key, either configured task type or path of class");
                }
            }
        }
    }

    public void updateSetting(final Settings settings) {
        Map<String, Settings> groups = settings.getAsGroups();
        for(String key : groups.keySet()) {
            if(taskNameToClassMapping.containsKey(key)) {
                updateLimit(taskNameToClassMapping.get(key), groups.get(key).getAsInt("value", DEFAULT_THRESHOLD_VALUE));
            } else {
                try {
                    updateLimit(Class.forName(key.replace("/", ".")),
                            groups.get(key).getAsInt("value", DEFAULT_THRESHOLD_VALUE));
                } catch (ClassNotFoundException e) {
                    // Ideally it should not throw this exception, as we are already validating this in validateSetting.
                    logger.error("Failed to apply validated master throttler setting, exception: [{}] ", e.getLocalizedMessage());
                }
            }
        }
    }

    @Override
    public boolean acquire(final Class type, final int permits) {
        boolean ableToAcquire = super.acquire(type, permits);
        if(!ableToAcquire) {
            throttlingStats.incrementThrottlingCount(type, permits);
        }
        return ableToAcquire;
    }

    public MasterThrottlingStats getThrottlingStats() {
        return throttlingStats;
    }

    protected void updateLimit(final Class className, final int limit) {
        assert limit >= DEFAULT_THRESHOLD_VALUE;
        if(limit == DEFAULT_THRESHOLD_VALUE) {
            super.removeThrottlingLimit(className);
        } else {
            super.updateThrottlingLimit(className, limit);
        }
    }
}
