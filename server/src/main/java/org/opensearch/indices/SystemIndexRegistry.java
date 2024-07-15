/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.regex.Regex;
import org.opensearch.tasks.TaskResultsService;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.opensearch.tasks.TaskResultsService.TASK_INDEX;

/**
 * This class holds the {@link SystemIndexDescriptor} objects that represent system indices the
 * node knows about. This class also contains static methods that identify if index expressions match
 * registered system index patterns
 *
 * @opensearch.api
 */
@ExperimentalApi
public class SystemIndexRegistry {
    private static final SystemIndexDescriptor TASK_INDEX_DESCRIPTOR = new SystemIndexDescriptor(TASK_INDEX + "*", "Task Result Index");
    private static final Map<String, Collection<SystemIndexDescriptor>> SERVER_SYSTEM_INDEX_DESCRIPTORS = singletonMap(
        TaskResultsService.class.getName(),
        singletonList(TASK_INDEX_DESCRIPTOR)
    );

    private volatile static String[] SYSTEM_INDEX_PATTERNS = new String[0];
    volatile static Collection<SystemIndexDescriptor> SYSTEM_INDEX_DESCRIPTORS = Collections.emptyList();
    volatile static Map<String, Collection<SystemIndexDescriptor>> SYSTEM_INDEX_DESCRIPTORS_MAP = Collections.emptyMap();

    static void register(Map<String, Collection<SystemIndexDescriptor>> pluginAndModulesDescriptors) {
        final Map<String, Collection<SystemIndexDescriptor>> descriptorsMap = buildSystemIndexDescriptorMap(pluginAndModulesDescriptors);
        checkForOverlappingPatterns(descriptorsMap);
        List<SystemIndexDescriptor> descriptors = pluginAndModulesDescriptors.values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        descriptors.add(TASK_INDEX_DESCRIPTOR);

        SYSTEM_INDEX_DESCRIPTORS_MAP = descriptorsMap;
        SYSTEM_INDEX_DESCRIPTORS = descriptors.stream().collect(Collectors.toUnmodifiableList());
        SYSTEM_INDEX_PATTERNS = descriptors.stream().map(SystemIndexDescriptor::getIndexPattern).toArray(String[]::new);
    }

    public static Set<String> matchesSystemIndexPattern(Set<String> indexExpressions) {
        return indexExpressions.stream().filter(pattern -> Regex.simpleMatch(SYSTEM_INDEX_PATTERNS, pattern)).collect(Collectors.toSet());
    }

    public static Set<String> matchesPluginSystemIndexPattern(String pluginClassName, Set<String> indexExpressions) {
        if (!SYSTEM_INDEX_DESCRIPTORS_MAP.containsKey(pluginClassName)) {
            return Collections.emptySet();
        }
        String[] pluginSystemIndexPatterns = SYSTEM_INDEX_DESCRIPTORS_MAP.get(pluginClassName)
            .stream()
            .map(SystemIndexDescriptor::getIndexPattern)
            .toArray(String[]::new);
        return indexExpressions.stream()
            .filter(pattern -> Regex.simpleMatch(pluginSystemIndexPatterns, pattern))
            .collect(Collectors.toSet());
    }

    /**
     * Given a collection of {@link SystemIndexDescriptor}s and their sources, checks to see if the index patterns of the listed
     * descriptors overlap with any of the other patterns. If any do, throws an exception.
     *
     * @param sourceToDescriptors A map of source (plugin) names to the SystemIndexDescriptors they provide.
     * @throws IllegalStateException Thrown if any of the index patterns overlaps with another.
     */
    static void checkForOverlappingPatterns(Map<String, Collection<SystemIndexDescriptor>> sourceToDescriptors) {
        List<Tuple<String, SystemIndexDescriptor>> sourceDescriptorPair = sourceToDescriptors.entrySet()
            .stream()
            .flatMap(entry -> entry.getValue().stream().map(descriptor -> new Tuple<>(entry.getKey(), descriptor)))
            .sorted(Comparator.comparing(d -> d.v1() + ":" + d.v2().getIndexPattern())) // Consistent ordering -> consistent error message
            .collect(Collectors.toList());

        // This is O(n^2) with the number of system index descriptors, and each check is quadratic with the number of states in the
        // automaton, but the absolute number of system index descriptors should be quite small (~10s at most), and the number of states
        // per pattern should be low as well. If these assumptions change, this might need to be reworked.
        sourceDescriptorPair.forEach(descriptorToCheck -> {
            List<Tuple<String, SystemIndexDescriptor>> descriptorsMatchingThisPattern = sourceDescriptorPair.stream()

                .filter(d -> descriptorToCheck.v2() != d.v2()) // Exclude the pattern currently being checked
                .filter(d -> overlaps(descriptorToCheck.v2(), d.v2()))
                .collect(Collectors.toList());
            if (descriptorsMatchingThisPattern.isEmpty() == false) {
                throw new IllegalStateException(
                    "a system index descriptor ["
                        + descriptorToCheck.v2()
                        + "] from ["
                        + descriptorToCheck.v1()
                        + "] overlaps with other system index descriptors: ["
                        + descriptorsMatchingThisPattern.stream()
                            .map(descriptor -> descriptor.v2() + " from [" + descriptor.v1() + "]")
                            .collect(Collectors.joining(", "))
                );
            }
        });
    }

    private static boolean overlaps(SystemIndexDescriptor a1, SystemIndexDescriptor a2) {
        Automaton a1Automaton = Regex.simpleMatchToAutomaton(a1.getIndexPattern());
        Automaton a2Automaton = Regex.simpleMatchToAutomaton(a2.getIndexPattern());
        return Operations.isEmpty(Operations.intersection(a1Automaton, a2Automaton)) == false;
    }

    private static Map<String, Collection<SystemIndexDescriptor>> buildSystemIndexDescriptorMap(
        Map<String, Collection<SystemIndexDescriptor>> pluginAndModulesMap
    ) {
        final Map<String, Collection<SystemIndexDescriptor>> map = new HashMap<>(
            pluginAndModulesMap.size() + SERVER_SYSTEM_INDEX_DESCRIPTORS.size()
        );
        map.putAll(pluginAndModulesMap);
        // put the server items last since we expect less of them
        SERVER_SYSTEM_INDEX_DESCRIPTORS.forEach((source, descriptors) -> {
            if (map.putIfAbsent(source, descriptors) != null) {
                throw new IllegalArgumentException(
                    "plugin or module attempted to define the same source [" + source + "] as a built-in system index"
                );
            }
        });
        return unmodifiableMap(map);
    }
}
