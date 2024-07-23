/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.regex.Regex;
import org.opensearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common Utility methods for Indices.
 *
 * @opensearch.internal
 */
public class IndexUtils {

    /**
     * Filters out list of available indices based on the list of selected indices.
     *
     * @param availableIndices list of available indices
     * @param selectedIndices  list of selected indices
     * @param indicesOptions    ignore indices flag
     * @return filtered out indices
     */
    public static List<String> filterIndices(List<String> availableIndices, String[] selectedIndices, IndicesOptions indicesOptions) {
        if (IndexNameExpressionResolver.isAllIndices(Arrays.asList(selectedIndices))) {
            return availableIndices;
        }

        // Move the exclusions to end of list to ensure they are processed
        // after explicitly selected indices are chosen.
        final List<String> excludesAtEndSelectedIndices = Stream.concat(
            Arrays.stream(selectedIndices).filter(s -> s.isEmpty() || s.charAt(0) != '-'),
            Arrays.stream(selectedIndices).filter(s -> !s.isEmpty() && s.charAt(0) == '-')
        ).collect(Collectors.toUnmodifiableList());

        Set<String> result = null;
        for (int i = 0; i < excludesAtEndSelectedIndices.size(); i++) {
            String indexOrPattern = excludesAtEndSelectedIndices.get(i);
            boolean add = true;
            if (!indexOrPattern.isEmpty()) {
                if (availableIndices.contains(indexOrPattern)) {
                    if (result == null) {
                        result = new HashSet<>();
                    }
                    result.add(indexOrPattern);
                    continue;
                }
                if (indexOrPattern.charAt(0) == '+') {
                    add = true;
                    indexOrPattern = indexOrPattern.substring(1);
                    // if its the first, add empty set
                    if (i == 0) {
                        result = new HashSet<>();
                    }
                } else if (indexOrPattern.charAt(0) == '-') {
                    // If the first index pattern is an exclusion, then all patterns are exclusions due to the
                    // reordering logic above. In this case, the request is interpreted as "include all indexes except
                    // those matching the exclusions" so we add all indices here and then remove the ones that match the exclusion patterns.
                    if (i == 0) {
                        result = new HashSet<>(availableIndices);
                    }
                    add = false;
                    indexOrPattern = indexOrPattern.substring(1);
                }
            }
            if (indexOrPattern.isEmpty() || !Regex.isSimpleMatchPattern(indexOrPattern)) {
                if (!availableIndices.contains(indexOrPattern)) {
                    if (!indicesOptions.ignoreUnavailable()) {
                        throw new IndexNotFoundException(indexOrPattern);
                    } else {
                        if (result == null) {
                            // add all the previous ones...
                            result = new HashSet<>(availableIndices.subList(0, i));
                        }
                    }
                } else {
                    if (result != null) {
                        if (add) {
                            result.add(indexOrPattern);
                        } else {
                            result.remove(indexOrPattern);
                        }
                    }
                }
                continue;
            }
            if (result == null) {
                // add all the previous ones...
                result = new HashSet<>(availableIndices.subList(0, i));
            }
            boolean found = false;
            for (String index : availableIndices) {
                if (Regex.simpleMatch(indexOrPattern, index)) {
                    found = true;
                    if (add) {
                        result.add(index);
                    } else {
                        result.remove(index);
                    }
                }
            }
            if (!found && !indicesOptions.allowNoIndices()) {
                throw new IndexNotFoundException(indexOrPattern);
            }
        }
        if (result == null) {
            return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(selectedIndices)));
        }
        return Collections.unmodifiableList(new ArrayList<>(result));
    }

}
