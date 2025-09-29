/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class AliasWriteIndexPolicyTests extends OpenSearchTestCase {

    public void testApplyAliasPolicyStripWriteIndex() {
        // Create snapshot metadata with write alias
        IndexMetadata.Builder indexBuilder = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("my-alias").writeIndex(true).build());

        IndexMetadata snapshotIndex = indexBuilder.build();

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().indices("test-index")
            .includeAliases(true)
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX);

        // Apply policy
        IndexMetadata.Builder restoredIndexBuilder = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
        Set<String> aliases = new HashSet<>();

        // Simulate the alias transformation
        for (AliasMetadata alias : snapshotIndex.getAliases().values()) {
            AliasMetadata transformed = RestoreService.applyAliasWriteIndexPolicy(alias, request.aliasWriteIndexPolicy());
            restoredIndexBuilder.putAlias(transformed);
            aliases.add(transformed.alias());
        }

        IndexMetadata restoredIndex = restoredIndexBuilder.build();
        AliasMetadata restoredAlias = restoredIndex.getAliases().get("my-alias");

        assertThat(restoredAlias, notNullValue());
        assertThat(restoredAlias.writeIndex(), equalTo(false));
        assertTrue(aliases.contains("my-alias"));
    }

    public void testApplyAliasPolicyWithRenameAndStrip() {
        IndexMetadata.Builder indexBuilder = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("old-alias").writeIndex(true).build());

        IndexMetadata snapshotIndex = indexBuilder.build();

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().indices("test-index")
            .includeAliases(true)
            .renameAliasPattern("old-(.+)")
            .renameAliasReplacement("new-$1")
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX);

        // Apply rename then policy
        AliasMetadata originalAlias = snapshotIndex.getAliases().values().iterator().next();
        AliasMetadata renamedAlias = AliasMetadata.newAliasMetadata(originalAlias, "new-alias");
        AliasMetadata transformed = RestoreService.applyAliasWriteIndexPolicy(renamedAlias, request.aliasWriteIndexPolicy());

        assertThat(transformed.alias(), equalTo("new-alias"));
        assertThat(transformed.writeIndex(), equalTo(false));
    }

    public void testApplyAliasPolicyPreserve() {
        IndexMetadata.Builder indexBuilder = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("my-alias").writeIndex(true).build());

        IndexMetadata snapshotIndex = indexBuilder.build();

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().indices("test-index")
            .includeAliases(true)
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.PRESERVE);

        // Apply policy
        IndexMetadata.Builder restoredIndexBuilder = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );

        for (AliasMetadata alias : snapshotIndex.getAliases().values()) {
            AliasMetadata transformed = RestoreService.applyAliasWriteIndexPolicy(alias, request.aliasWriteIndexPolicy());
            restoredIndexBuilder.putAlias(transformed);
        }

        IndexMetadata restoredIndex = restoredIndexBuilder.build();
        AliasMetadata restoredAlias = restoredIndex.getAliases().get("my-alias");

        assertThat(restoredAlias, notNullValue());
        assertThat(restoredAlias.writeIndex(), equalTo(true)); // Preserved
    }

    public void testApplyAliasPolicyWithMixedWriteIndexValues() {
        IndexMetadata.Builder indexBuilder = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .putAlias(AliasMetadata.builder("alias2").writeIndex(false).build())
            .putAlias(AliasMetadata.builder("alias3").build()); // null writeIndex

        IndexMetadata snapshotIndex = indexBuilder.build();

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().indices("test-index")
            .includeAliases(true)
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX);

        // Apply policy
        int processedCount = 0;
        for (AliasMetadata alias : snapshotIndex.getAliases().values()) {
            AliasMetadata transformed = RestoreService.applyAliasWriteIndexPolicy(alias, request.aliasWriteIndexPolicy());

            // All aliases should have writeIndex false or null after transformation
            if (alias.alias().equals("alias1")) {
                assertThat(transformed.writeIndex(), equalTo(false)); // was true, now false
            } else {
                assertThat(transformed.writeIndex(), equalTo(alias.writeIndex())); // unchanged
            }
            processedCount++;
        }

        assertThat(processedCount, equalTo(3));
    }

}
