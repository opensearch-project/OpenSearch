/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.Timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Simplified profiling tree for fetch phase operations. Each fetch phase is
 * stored as a root with a single level of child sub phases.
 *
 * @opensearch.internal
 */
class FlatFetchProfileTree {
    private static final Set<String> ROOT_KEYS = Set.of(
        FetchTimingType.CREATE_STORED_FIELDS_VISITOR.toString(),
        FetchTimingType.CREATE_STORED_FIELDS_VISITOR.toString() + Timer.TIMING_TYPE_COUNT_SUFFIX,
        FetchTimingType.BUILD_SUB_PHASE_PROCESSORS.toString(),
        FetchTimingType.BUILD_SUB_PHASE_PROCESSORS.toString() + Timer.TIMING_TYPE_COUNT_SUFFIX,
        FetchTimingType.LOAD_STORED_FIELDS.toString(),
        FetchTimingType.LOAD_STORED_FIELDS.toString() + Timer.TIMING_TYPE_COUNT_SUFFIX,
        FetchTimingType.LOAD_SOURCE.toString(),
        FetchTimingType.LOAD_SOURCE.toString() + Timer.TIMING_TYPE_COUNT_SUFFIX,
        FetchTimingType.GET_NEXT_READER.toString(),
        FetchTimingType.GET_NEXT_READER.toString() + Timer.TIMING_TYPE_COUNT_SUFFIX
    );

    private static final Set<String> SUB_PHASE_KEYS = Set.of(
        FetchTimingType.PROCESS.toString(),
        FetchTimingType.PROCESS.toString() + Timer.TIMING_TYPE_COUNT_SUFFIX,
        FetchTimingType.SET_NEXT_READER.toString(),
        FetchTimingType.SET_NEXT_READER.toString() + Timer.TIMING_TYPE_COUNT_SUFFIX
    );

    private static class Node {
        final String element;
        final FetchProfileBreakdown breakdown;
        final List<Node> children = new ArrayList<>();

        Node(String element) {
            this.element = element;
            this.breakdown = new FetchProfileBreakdown();
        }
    }

    private final List<Node> roots = new ArrayList<>();
    private final Map<String, Node> phaseMap = new HashMap<>();

    /** Start profiling a new fetch phase and return its breakdown. */
    FetchProfileBreakdown startFetchPhase(String element) {
        Node node = new Node(element);
        roots.add(node);
        phaseMap.put(element, node);
        return node.breakdown;
    }

    /** Start profiling a fetch sub-phase under the specified parent phase. */
    FetchProfileBreakdown startSubPhase(String element, String parentElement) {
        Node parent = phaseMap.get(parentElement);
        if (parent == null) {
            throw new IllegalStateException("Parent phase '" + parentElement + "' does not exist for sub-phase '" + element + "'");
        }
        Node child = new Node(element);
        parent.children.add(child);
        return child.breakdown;
    }

    /**
     * Finish profiling of the specified fetch phase.
     */
    void endFetchPhase(String element) {
        phaseMap.remove(element);
    }

    /**
     * Build the profile results tree for serialization.
     */
    List<ProfileResult> getTree() {
        List<ProfileResult> results = new ArrayList<>(roots.size());
        for (Node root : roots) {
            results.add(toProfileResult(root, true));
        }
        return results;
    }

    private ProfileResult toProfileResult(Node node, boolean isRoot) {
        List<ProfileResult> children = new ArrayList<>(node.children.size());
        for (Node child : node.children) {
            children.add(toProfileResult(child, false));
        }
        Map<String, Long> raw = node.breakdown.toBreakdownMap();
        Map<String, Long> filtered = filterBreakdown(raw, isRoot);
        return new ProfileResult(node.element, node.element, filtered, node.breakdown.toDebugMap(), inclusiveTime(node), children);
    }

    private long inclusiveTime(Node node) {
        long total = node.breakdown.toNodeTime();
        for (Node child : node.children) {
            total += inclusiveTime(child);
        }
        return total;
    }

    private Map<String, Long> filterBreakdown(Map<String, Long> raw, boolean isRoot) {
        Set<String> allowed = isRoot ? ROOT_KEYS : SUB_PHASE_KEYS;
        Map<String, Long> map = new TreeMap<>();
        for (String key : allowed) {
            map.put(key, raw.getOrDefault(key, 0L));
        }
        return map;
    }
}
