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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.snapshots;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.regex.Regex;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Snapshot utilities
 *
 * @opensearch.internal
 */
public class SnapshotUtils {

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

    /**
     * Validates if there are any remote snapshots backing an index
     * @param metadata index metadata from cluster state
     * @param snapshotIds list of snapshot Ids to be verified
     * @param repoName repo name for which the verification is being done
     */
    public static void validateSnapshotsBackingAnyIndex(
        final Map<String, IndexMetadata> metadata,
        List<SnapshotId> snapshotIds,
        String repoName
    ) {
        final Map<String, SnapshotId> uuidToSnapshotId = new HashMap<>();
        final Set<String> snapshotsToBeNotDeleted = new HashSet<>();
        snapshotIds.forEach(snapshotId -> uuidToSnapshotId.put(snapshotId.getUUID(), snapshotId));

        for (final IndexMetadata indexMetadata : metadata.values()) {
            String storeType = indexMetadata.getSettings().get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());
            if (IndexModule.Type.REMOTE_SNAPSHOT.match(storeType)) {
                String snapshotId = indexMetadata.getSettings().get(IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.getKey());
                if (uuidToSnapshotId.get(snapshotId) != null) {
                    snapshotsToBeNotDeleted.add(uuidToSnapshotId.get(snapshotId).getName());
                }
            }
        }

        if (!snapshotsToBeNotDeleted.isEmpty()) {
            throw new SnapshotInUseDeletionException(
                repoName,
                snapshotsToBeNotDeleted.toString(),
                "These remote snapshots are backing some indices and hence can't be deleted! No snapshots were deleted."
            );
        }
    }
}
