/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolved, hot-reloadable view of the per-backend delegation block-list configured via
 * {@link AnalyticsQuerySettings#DELEGATION_BLOCKED_PREDICATES}. Holds the operator-facing
 * {@code Map<BackendName, List<BlockedPredicate>>} as a {@code Map<String, EnumSet<ScalarFunction>>}
 * so the marking-time hot path ({@link #isBlocked}) is an enum-set membership check.
 *
 * <p>The vocabulary is <b>derived from the runtime backend registry</b>, not a parallel enum: a
 * backend namespace is valid only if the backend <em>accepts</em> FILTER delegation
 * ({@link CapabilityRegistry#delegationAcceptors}), and a predicate is valid only if that backend
 * ships a serializer for it ({@code delegatedPredicateSerializers().keySet()}). Today Lucene is the
 * only FILTER-delegation acceptor, so any other namespace (e.g. {@code datafusion}) is rejected.
 * Validation runs at <b>setting-update time</b> (the affix update consumer / initial seed), so an
 * invalid backend or unsupported predicate fails the cluster-settings update with a clear message.
 *
 * @opensearch.internal
 */
public final class DelegationBlockList {

    private static final Logger LOGGER = LogManager.getLogger(DelegationBlockList.class);

    /** backendName -> blocked predicate functions. Absent/empty entry ⇒ nothing blocked. */
    private final Map<String, EnumSet<ScalarFunction>> blockedByBackend;

    private DelegationBlockList(Map<String, EnumSet<ScalarFunction>> blockedByBackend) {
        this.blockedByBackend = blockedByBackend;
    }

    /** An empty block-list — nothing is blocked. */
    public static DelegationBlockList empty() {
        return new DelegationBlockList(new ConcurrentHashMap<>());
    }

    /** Test/seed factory from a plain map (assumes pre-validated input). */
    public static DelegationBlockList fromMap(Map<String, List<ScalarFunction>> seed) {
        Map<String, EnumSet<ScalarFunction>> map = new ConcurrentHashMap<>();
        for (Map.Entry<String, List<ScalarFunction>> e : seed.entrySet()) {
            map.put(e.getKey(), toEnumSet(e.getValue()));
        }
        return new DelegationBlockList(map);
    }

    /**
     * Production factory: seeds from the current cluster settings and registers an affix update
     * consumer + validator so later {@code analytics.delegation.<backend>.blocked_predicates} changes
     * are validated against {@code registry} and applied live. The validator rejects (1) a namespace
     * that is not a FILTER-delegation acceptor and (2) a predicate the acceptor has no serializer for.
     */
    public static DelegationBlockList create(ClusterSettings clusterSettings, Settings initialSettings, CapabilityRegistry registry) {
        Map<String, EnumSet<ScalarFunction>> map = new ConcurrentHashMap<>();
        DelegationBlockList blockList = new DelegationBlockList(map);
        Map<String, List<ScalarFunction>> initial = AnalyticsQuerySettings.DELEGATION_BLOCKED_PREDICATES.getAsMap(initialSettings);
        // Seed missing namespaces with setting defaults (getAsMap skips unset keys).
        for (String acceptor : registry.delegationAcceptors(DelegationType.FILTER)) {
            if (!initial.containsKey(acceptor)) {
                Setting<List<ScalarFunction>> concrete = AnalyticsQuerySettings.DELEGATION_BLOCKED_PREDICATES
                    .getConcreteSettingForNamespace(acceptor);
                List<ScalarFunction> defaults = concrete.get(initialSettings);
                if (!defaults.isEmpty()) {
                    Set<ScalarFunction> delegatable = registry.getBackend(acceptor).delegatedPredicateSerializers().keySet();
                    List<ScalarFunction> applicable = defaults.stream().filter(delegatable::contains).toList();
                    if (!applicable.isEmpty()) {
                        initial = new HashMap<>(initial);
                        initial.put(acceptor, applicable);
                    }
                }
            }
        }
        for (Map.Entry<String, List<ScalarFunction>> e : initial.entrySet()) {
            validate(registry, e.getKey(), e.getValue());
            if (!e.getValue().isEmpty()) {
                map.put(e.getKey(), toEnumSet(e.getValue()));
            }
        }
        clusterSettings.addAffixUpdateConsumer(
            AnalyticsQuerySettings.DELEGATION_BLOCKED_PREDICATES,
            blockList::update,
            (backendName, predicates) -> validate(registry, backendName, predicates)
        );
        return blockList;
    }

    /**
     * Rejects an invalid block-list entry: an unknown delegation-target backend, or a predicate the
     * target backend cannot delegate (no serializer). Called by the affix update validator, so a bad
     * value fails the cluster-settings update before {@link #update} ever runs.
     */
    private static void validate(CapabilityRegistry registry, String backendName, List<ScalarFunction> predicates) {
        if (predicates.isEmpty()) {
            return; // clearing a namespace is always allowed
        }
        List<String> acceptors = registry.delegationAcceptors(DelegationType.FILTER);
        if (!acceptors.contains(backendName)) {
            throw new IllegalArgumentException(
                "analytics.delegation."
                    + backendName
                    + ".blocked_predicates: ["
                    + backendName
                    + "] is not a delegation-target backend; FILTER delegation acceptors are "
                    + acceptors
            );
        }
        Set<ScalarFunction> delegatable = registry.getBackend(backendName).delegatedPredicateSerializers().keySet();
        for (ScalarFunction predicate : predicates) {
            if (!delegatable.contains(predicate)) {
                throw new IllegalArgumentException(
                    "analytics.delegation."
                        + backendName
                        + ".blocked_predicates: ["
                        + predicate
                        + "] is not delegatable to ["
                        + backendName
                        + "]; delegatable predicates are "
                        + delegatable
                );
            }
        }
    }

    /** Update (or clear) the blocked set for one backend namespace. Called by the settings-update thread. */
    private void update(String backendName, List<ScalarFunction> blocked) {
        if (blocked == null || blocked.isEmpty()) {
            blockedByBackend.remove(backendName);
        } else {
            blockedByBackend.put(backendName, toEnumSet(blocked));
        }
        LOGGER.info("Delegation block-list updated: backend [{}] now blocks {}", backendName, blocked);
    }

    /** True iff {@code predicate} is blocked from delegation to {@code backendName}. */
    public boolean isBlocked(String backendName, ScalarFunction predicate) {
        EnumSet<ScalarFunction> set = blockedByBackend.get(backendName);
        return set != null && set.contains(predicate);
    }

    /** True iff there is at least one block entry (lets callers skip per-predicate work when empty). */
    public boolean isEmpty() {
        return blockedByBackend.isEmpty();
    }

    private static EnumSet<ScalarFunction> toEnumSet(List<ScalarFunction> values) {
        EnumSet<ScalarFunction> set = EnumSet.noneOf(ScalarFunction.class);
        set.addAll(values);
        return set;
    }
}
