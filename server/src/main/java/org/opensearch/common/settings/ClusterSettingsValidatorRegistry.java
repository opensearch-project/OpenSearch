/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.regex.Regex;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.script.ScriptService;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Validates proposed cluster settings against cluster state before the state is published.
 *
 * @opensearch.internal
 */
@InternalApi
public final class ClusterSettingsValidatorRegistry {
    private final Settings nodeSettings;
    private final Map<Setting<?>, BiConsumer<Settings, ClusterState>> validators;

    public ClusterSettingsValidatorRegistry(
        Settings nodeSettings,
        Map<Setting<?>, BiConsumer<Settings, ClusterState>> additionalValidators
    ) {
        this.nodeSettings = nodeSettings;
        Map<Setting<?>, BiConsumer<Settings, ClusterState>> validators = new LinkedHashMap<>();
        validators.put(
            ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING,
            ShardsLimitAllocationDecider::validateClusterTotalPrimaryShardsPerNodeSetting
        );
        validators.put(
            RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING,
            RemoteStoreNodeService::validateCompatibilityModeSetting
        );
        validators.put(
            ScriptService.SCRIPT_MAX_SIZE_IN_BYTES,
            (settings, state) -> ScriptService.validateMaxSizeInBytes(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(settings), state)
        );
        additionalValidators.forEach((setting, validator) -> {
            Objects.requireNonNull(validator, "cluster settings validator");
            if (setting.hasNodeScope() == false || setting.isDynamic() == false) {
                throw new IllegalArgumentException("Cluster settings validator requires a dynamic node setting [" + setting.getKey() + "]");
            }
            if (validators.putIfAbsent(setting, validator) != null) {
                throw new IllegalArgumentException("Cluster settings validator for [" + setting.getKey() + "] is already registered");
            }
        });
        this.validators = Collections.unmodifiableMap(validators);
    }

    /**
     * Runs validators only for settings or their fallbacks touched by the request, including resets. Validators receive node settings
     * overlaid with the proposed persistent and transient settings, and the final proposed cluster state.
     */
    public void validate(Settings transientUpdates, Settings persistentUpdates, ClusterState currentState, ClusterState updatedState) {
        Settings effectiveSettings = Settings.builder().put(nodeSettings).put(updatedState.metadata().settings()).build();
        for (Map.Entry<Setting<?>, BiConsumer<Settings, ClusterState>> entry : validators.entrySet()) {
            Setting<?> setting = entry.getKey();
            if (isUpdated(setting, transientUpdates, currentState.metadata().transientSettings())
                || isUpdated(setting, persistentUpdates, currentState.metadata().persistentSettings())) {
                entry.getValue().accept(effectiveSettings, updatedState);
            }
        }
    }

    /**
     * Returns whether a request touches a setting or one of its fallbacks, expanding wildcard resets against existing concrete keys.
     */
    public static boolean isUpdated(Setting<?> setting, Settings updates, Settings currentSettings) {
        for (Setting<?> relevantSetting = setting; relevantSetting != null; relevantSetting = relevantSetting.fallbackSetting) {
            for (String key : updates.keySet()) {
                if (relevantSetting.match(key)) {
                    return true;
                }
                if (updates.hasValue(key) == false && key.endsWith("*")) {
                    if (Regex.simpleMatch(key, relevantSetting.getKey())) {
                        return true;
                    }
                    // Expand wildcard resets against existing keys so affix settings are validated when their concrete
                    // values are removed from the proposed state.
                    for (String currentKey : currentSettings.keySet()) {
                        if (relevantSetting.match(currentKey) && Regex.simpleMatch(key, currentKey)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
