/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.registry;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Maps backend engine names to the set of relational operators and scalar functions
 * each engine supports. Used by the query planner to make backend assignment decisions.
 *
 * <p>Backends are stored in insertion order (via {@link LinkedHashMap}), which defines
 * priority: the first registered backend has the highest priority.
 *
 * <p>Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6
 */
public final class BackendCapabilityRegistry {

    // insertion-ordered: backendName → supported operator Class set
    private final LinkedHashMap<String, Set<Class<? extends RelNode>>> operatorSupport = new LinkedHashMap<>();
    // insertion-ordered: backendName → supported SqlOperator names (upper-cased)
    private final LinkedHashMap<String, Set<String>> functionSupport = new LinkedHashMap<>();
    // insertion-ordered: backendName → plugin instance (for canAcceptUnresolvedPredicate)
    private final LinkedHashMap<String, AnalyticsBackEndPlugin> plugins = new LinkedHashMap<>();

    /**
     * Registers a backend with its supported operator types and function names.
     * If a backend with the same name is already registered, the existing entry is overwritten.
     */
    public void register(String backendName,
                         Set<Class<? extends RelNode>> supportedOperators,
                         Set<String> supportedFunctionNames) {
        operatorSupport.put(backendName, Set.copyOf(supportedOperators));
        functionSupport.put(backendName, Set.copyOf(supportedFunctionNames));
    }

    /**
     * Registers a backend with its plugin instance (for unresolved predicate resolution).
     */
    public void register(String backendName,
                         Set<Class<? extends RelNode>> supportedOperators,
                         Set<String> supportedFunctionNames,
                         AnalyticsBackEndPlugin plugin) {
        register(backendName, supportedOperators, supportedFunctionNames);
        plugins.put(backendName, plugin);
    }

    /** Removes all entries for the given backend name. */
    public void deregister(String backendName) {
        operatorSupport.remove(backendName);
        functionSupport.remove(backendName);
        plugins.remove(backendName);
    }

    /**
     * Returns backend names that support the given operator class, in priority (insertion) order.
     * Matches by comparing class hierarchies to handle classloader isolation and
     * wrapped-vs-logical operator mismatches (e.g. OpenSearchTableScan vs LogicalTableScan
     * both extend org.apache.calcite.rel.core.TableScan).
     */
    public List<String> backendsForOperator(Class<? extends RelNode> operatorClass) {
        Set<String> queryAncestors = calciteAncestors(operatorClass);
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, Set<Class<? extends RelNode>>> entry : operatorSupport.entrySet()) {
            for (Class<? extends RelNode> supported : entry.getValue()) {
                Set<String> supportedAncestors = calciteAncestors(supported);
                // Match if they share any Calcite ancestor or have the same name
                if (supported.getName().equals(operatorClass.getName())
                    || !disjoint(queryAncestors, supportedAncestors)) {
                    result.add(entry.getKey());
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Collects fully-qualified names of all Calcite ancestor classes
     * (package starts with org.apache.calcite) in the class hierarchy.
     */
    private static Set<String> calciteAncestors(Class<?> clazz) {
        Set<String> ancestors = new java.util.HashSet<>();
        Class<?> current = clazz;
        while (current != null && RelNode.class.isAssignableFrom(current)) {
            if (current.getName().startsWith("org.apache.calcite.")) {
                ancestors.add(current.getName());
            }
            current = current.getSuperclass();
        }
        return ancestors;
    }

    private static boolean disjoint(Set<String> a, Set<String> b) {
        for (String s : a) {
            if (b.contains(s)) return false;
        }
        return true;
    }

    /**
     * Returns backend names that support the given SQL function name, in priority order.
     */
    public List<String> backendsForFunction(String functionName) {
        String upper = functionName.toUpperCase(java.util.Locale.ROOT);
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : functionSupport.entrySet()) {
            if (entry.getValue().contains(upper)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Queries each registered backend plugin in priority order to find one that accepts
     * the given opaque predicate payload.
     *
     * @return the name of the first backend that accepts the payload, or empty if none do
     */
    public Optional<String> backendForUnresolvedPredicate(byte[] payload) {
        for (Map.Entry<String, AnalyticsBackEndPlugin> entry : plugins.entrySet()) {
            if (entry.getValue().canAcceptUnresolvedPredicate(payload)) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }
}
