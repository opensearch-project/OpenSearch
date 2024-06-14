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

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot utilities
 *
 * @opensearch.internal
 */
public class SnapshotUtils {

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
