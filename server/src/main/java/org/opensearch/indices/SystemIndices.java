/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.common.Nullable;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.index.Index;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class holds the {@link SystemIndexDescriptor} objects that represent system indices the
 * node knows about. Methods for determining if an index should be a system index are also provided
 * to reduce the locations within the code that need to deal with {@link SystemIndexDescriptor}s.
 *
 * @opensearch.internal
 */
public class SystemIndices {
    private static final Logger logger = LogManager.getLogger(SystemIndices.class);

    private final CharacterRunAutomaton runAutomaton;

    public SystemIndices(Map<String, Collection<SystemIndexDescriptor>> pluginAndModulesDescriptors) {
        SystemIndexRegistry.register(pluginAndModulesDescriptors);
        this.runAutomaton = buildCharacterRunAutomaton(SystemIndexRegistry.SYSTEM_INDEX_DESCRIPTORS);
    }

    /**
     * Determines whether a given index is a system index by comparing its name to the collection of loaded {@link SystemIndexDescriptor}s
     * @param index the {@link Index} object to check against loaded {@link SystemIndexDescriptor}s
     * @return true if the {@link Index}'s name matches a pattern from a {@link SystemIndexDescriptor}
     */
    public boolean isSystemIndex(Index index) {
        return isSystemIndex(index.getName());
    }

    /**
     * Determines whether a given index is a system index by comparing its name to the collection of loaded {@link SystemIndexDescriptor}s
     * @param indexName the index name to check against loaded {@link SystemIndexDescriptor}s
     * @return true if the index name matches a pattern from a {@link SystemIndexDescriptor}
     */
    public boolean isSystemIndex(String indexName) {
        return runAutomaton.run(indexName);
    }

    /**
     * Finds a single matching {@link SystemIndexDescriptor}, if any, for the given index name.
     * @param name the name of the index
     * @return The matching {@link SystemIndexDescriptor} or {@code null} if no descriptor is found
     * @throws IllegalStateException if multiple descriptors match the name
     */
    public @Nullable SystemIndexDescriptor findMatchingDescriptor(String name) {
        final List<SystemIndexDescriptor> matchingDescriptors = SystemIndexRegistry.SYSTEM_INDEX_DESCRIPTORS.stream()
            .filter(descriptor -> descriptor.matchesIndexPattern(name))
            .collect(Collectors.toList());

        if (matchingDescriptors.isEmpty()) {
            return null;
        } else if (matchingDescriptors.size() == 1) {
            return matchingDescriptors.get(0);
        } else {
            // This should be prevented by failing on overlapping patterns at startup time, but is here just in case.
            StringBuilder errorMessage = new StringBuilder().append("index name [")
                .append(name)
                .append("] is claimed as a system index by multiple system index patterns: [")
                .append(
                    matchingDescriptors.stream()
                        .map(
                            descriptor -> "pattern: ["
                                + descriptor.getIndexPattern()
                                + "], description: ["
                                + descriptor.getDescription()
                                + "]"
                        )
                        .collect(Collectors.joining("; "))
                );
            // Throw AssertionError if assertions are enabled, or a regular exception otherwise:
            assert false : errorMessage.toString();
            throw new IllegalStateException(errorMessage.toString());
        }
    }

    /**
     * Validates (if this index has a dot-prefixed name) and it is system index.
     * @param index The name of the index in question
     */
    public boolean validateSystemIndex(String index) {
        if (index.charAt(0) == '.') {
            SystemIndexDescriptor matchingDescriptor = findMatchingDescriptor(index);
            if (matchingDescriptor != null) {
                logger.trace(
                    "index [{}] is a system index because it matches index pattern [{}] with description [{}]",
                    index,
                    matchingDescriptor.getIndexPattern(),
                    matchingDescriptor.getDescription()
                );
                return true;
            }
        }

        return false;
    }

    private static CharacterRunAutomaton buildCharacterRunAutomaton(Collection<SystemIndexDescriptor> descriptors) {
        Optional<Automaton> automaton = descriptors.stream()
            .map(descriptor -> Regex.simpleMatchToAutomaton(descriptor.getIndexPattern()))
            .reduce(Operations::union);
        return new CharacterRunAutomaton(MinimizationOperations.minimize(automaton.orElse(Automata.makeEmpty()), Integer.MAX_VALUE));
    }
}
