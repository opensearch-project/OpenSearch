/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * AutoExpandReplica Disabled for Search Replica
 */
public class DisableAutoExpandReplicaForSearchReplicaTests extends OpenSearchTestCase {

    public void testWhenAutoExpandReplicaAndSearchReplicaIsSetInTheRequest() {
        Settings conflictRequestSettings = Settings.builder()
            .put("index.number_of_search_only_replicas", 1)
            .put("index.auto_expand_replicas", "0-all")
            .build();

        assertEquals(
            "Cannot set both [index.auto_expand_replicas] and [index.number_of_search_only_replicas]. These settings are mutually exclusive.",
            MetadataCreateIndexService.validateAutoExpandReplicaConflictInRequest(conflictRequestSettings).get()
        );
    }

    public void testWhenOnlyAutoExpandReplicaIsSetInTheRequest() {
        Settings reqestSettings = Settings.builder()
            .put("index.number_of_search_only_replicas", 0)
            .put("index.auto_expand_replicas", "0-all")
            .build();

        assertTrue(MetadataCreateIndexService.validateAutoExpandReplicaConflictInRequest(reqestSettings).isEmpty());
    }

    public void testWhenAutoExpandReplicaEnabledForTheIndex() {
        Settings reqestSettings = Settings.builder().put("index.number_of_search_only_replicas", 1).build();

        Settings indexSettings = Settings.builder().put("index.auto_expand_replicas", "0-all").build();

        assertEquals(
            "Cannot set [index.number_of_search_only_replicas] because [index.auto_expand_replicas] is configured. These settings are mutually exclusive.",
            MetadataCreateIndexService.validateAutoExpandReplicaConflictWithIndex(reqestSettings, indexSettings).get()
        );
    }

    public void testWhenSearchReplicaEnabledForTheIndex() {
        Settings reqestSettings = Settings.builder().put("index.auto_expand_replicas", "0-all").build();

        Settings indexSettings = Settings.builder().put("index.number_of_search_only_replicas", 1).build();

        assertEquals(
            "Cannot set [index.auto_expand_replicas] because [index.number_of_search_only_replicas] is set. These settings are mutually exclusive.",
            MetadataCreateIndexService.validateAutoExpandReplicaConflictWithIndex(reqestSettings, indexSettings).get()
        );
    }

    public void testWhenSearchReplicaSetZeroForTheIndex() {
        Settings reqestSettings = Settings.builder().put("index.auto_expand_replicas", "0-all").build();

        Settings indexSettings = Settings.builder().put("index.number_of_search_only_replicas", 0).build();

        assertTrue(MetadataCreateIndexService.validateAutoExpandReplicaConflictWithIndex(reqestSettings, indexSettings).isEmpty());
    }

    public void testIfSearchReplicaEnabledEnablingAutoExpandReplicasAndDisablingSearchReplicasNotPossibleInSingleRequest() {
        Settings reqestSettings = Settings.builder()
            .put("index.auto_expand_replicas", "0-all")
            .put("index.number_of_search_only_replicas", 0)
            .build();

        Settings indexSettings = Settings.builder().put("index.number_of_search_only_replicas", 1).build();

        assertEquals(
            "Cannot set [index.auto_expand_replicas] because [index.number_of_search_only_replicas] "
                + "is set. These settings are mutually exclusive.",
            MetadataCreateIndexService.validateAutoExpandReplicaConflictWithIndex(reqestSettings, indexSettings).get()
        );
    }
}
