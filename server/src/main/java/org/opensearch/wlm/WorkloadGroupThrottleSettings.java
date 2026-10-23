/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Registry of valid workload group throttle settings with their validators. Throttle config is a nested
 * {@code throttling} object (a {@link Settings} bag) like {@code settings}, so per-key null clears a field and an
 * absent key keeps the existing value with no extra bookkeeping.
 */
@ExperimentalApi
public class WorkloadGroupThrottleSettings {

    /** Sentinel for an unset limit, matching the {@code -1 = not set} convention of {@code WLM_SEARCH_TIMEOUT}. */
    public static final int UNSET_LIMIT = -1;

    /** Internal dimension used when {@link #BY} is omitted: one bucket for the workload group as a whole. */
    public static final String GROUP_SCOPE = "group";

    /** {@link #BY} value giving each username its own bucket per node. */
    public static final String BY_USERNAME = "username";

    /** {@link #BY} value giving each role its own bucket per node. */
    public static final String BY_ROLE = "role";

    /**
     * Legal explicit {@link #BY} values. Group scope is deliberately absent: omitting {@code by} selects it, while a
     * per-key {@code null} on update clears a username/role override back to that default.
     */
    public static final Set<String> ALLOWED_BY_VALUES = Collections.unmodifiableSet(new LinkedHashSet<>(List.of(BY_USERNAME, BY_ROLE)));

    /**
     * Optional dimension used to subdivide the workload group's allowance. An omitted key resolves to
     * {@link #GROUP_SCOPE}; only {@code username} and {@code role} may be supplied explicitly.
     */
    public static final Setting<String> BY = Setting.simpleString("by", GROUP_SCOPE, new Setting.Validator<String>() {
        @Override
        public void validate(String value) {
            if (GROUP_SCOPE.equals(value) == false && ALLOWED_BY_VALUES.contains(value) == false) {
                throw new IllegalArgumentException(
                    "throttling.by must be one of " + ALLOWED_BY_VALUES + " when set but was '" + value + "'"
                );
            }
        }

        @Override
        public void validate(String value, Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent && GROUP_SCOPE.equals(value)) {
                throw new IllegalArgumentException(
                    "throttling.by must be one of " + ALLOWED_BY_VALUES + " when set but was '" + value + "'"
                );
            }
        }
    });

    /**
     * Per-node in-flight allowance admitted locally with no coordination. An absent value resolves to {@link #UNSET_LIMIT},
     * while an explicitly supplied value must be non-negative.
     */
    public static final Setting<Integer> NODE_LIMIT = Setting.intSetting(
        "node_limit",
        UNSET_LIMIT,
        Integer.MIN_VALUE,
        new Setting.Validator<Integer>() {
            @Override
            public void validate(Integer value) {}

            @Override
            public void validate(Integer value, Map<Setting<?>, Object> settings, boolean isPresent) {
                if (isPresent && value < 0) {
                    throw new IllegalArgumentException("throttling.node_limit must be non-negative but was " + value);
                }
            }
        }
    );

    private static final Map<String, Setting<?>> REGISTERED_SETTINGS = Map.of(BY.getKey(), BY, NODE_LIMIT.getKey(), NODE_LIMIT);

    private WorkloadGroupThrottleSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /** True for keys holding an integer limit, which xContent must emit as a JSON number rather than a string. */
    static boolean isLimitKey(String key) {
        return NODE_LIMIT.getKey().equals(key);
    }

    /**
     * Returns the effective bucket dimension, defaulting to the implicit group scope. Unknown keys are rejected so a
     * schema this node does not understand is not silently applied as group throttling.
     */
    public static String getEffectiveBy(Settings throttling) {
        if (throttling == null) {
            return GROUP_SCOPE;
        }
        for (String key : throttling.keySet()) {
            if (REGISTERED_SETTINGS.containsKey(key) == false) {
                throw new IllegalArgumentException("Unknown throttle setting: " + key);
            }
        }
        if (throttling.hasValue(BY.getKey()) == false) {
            return GROUP_SCOPE;
        }
        return BY.get(throttling);
    }

    /**
     * Per-key validation: every key must be registered, {@code by} an allowed value, and each limit a non-negative int.
     * Safe on a partial update fragment; cross-field checks live in {@link #validateMergedConfig(Settings)}.
     *
     * @param throttling the throttling settings to validate
     * @throws IllegalArgumentException if any key is unknown or any value is invalid
     */
    public static void validate(Settings throttling) {
        if (throttling == null) {
            return;
        }
        for (String key : throttling.keySet()) {
            String value = throttling.get(key);
            Setting<?> setting = REGISTERED_SETTINGS.get(key);
            if (setting == null) {
                throw new IllegalArgumentException("Unknown throttle setting: " + key);
            }
            // null value means "clear this key" — skip value validation
            if (value == null) {
                continue;
            }
            try {
                setting.get(Settings.builder().put(key, value).build());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid value '" + value + "' for throttling." + key + ": " + e.getMessage(), e);
            }
        }
    }

    /**
     * Cross-field validation on a fully-merged throttling config. When throttling is configured, {@code node_limit} is
     * required and its effective ceiling must be at least 1, since a ceiling of 0 rejects every request. The optional
     * {@code by} key defaults to group scope. Must be called on the merged result, not a partial update fragment.
     *
     * @param throttling the merged throttling settings
     * @throws IllegalArgumentException if a {@code by} value has no limit, or the effective ceiling is 0
     */
    public static void validateMergedConfig(Settings throttling) {
        if (throttling == null || throttling.isEmpty()) {
            return;
        }
        validate(throttling);
        boolean hasNode = throttling.hasValue(NODE_LIMIT.getKey());

        if (hasNode == false) {
            // A null-valued key is an update clear marker and configures nothing after merging.
            if (throttling.hasValue(BY.getKey())) {
                throw new IllegalArgumentException(
                    "throttling.node_limit is required when throttling.by is set; " + "set throttling as null to disable throttling instead"
                );
            }
            return;
        }
        int node = NODE_LIMIT.get(throttling);
        if (node < 1) {
            throw new IllegalArgumentException(
                "Effective throttle ceiling is 0 (node_limit="
                    + node
                    + "); this would reject all requests. "
                    + "Set node_limit to a positive value, or set throttling as null to disable throttling"
            );
        }
    }
}
