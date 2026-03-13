/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.Diff;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.Mockito;

import static org.opensearch.cluster.metadata.SplitShardsMetadata.validateShardRanges;
import static org.mockito.Mockito.when;

public class SplitShardsMetadataTests extends OpenSearchTestCase {

    public void testGetShardIdOfHashWithNoChildren() {
        // Setup
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);

        // Test: When there are no children, should return the root shard id
        int result = builder.build().getShardIdOfHash(0, 100, false);
        assertEquals(0, result);
    }

    public void testGetRootShards_splitInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);

        SplitShardsMetadata metadata = builder.build();
        List<Integer> rootShards = metadata.getRootShards();
        List<Integer> expectedRootShards = List.of(0, 1, 2);
        assertEquals(expectedRootShards, rootShards);
    }

    public void testGetRootShards_splitCompleted() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);

        // split
        builder.splitShard(0, 3);

        // split completed for shard 0
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4, 5));

        SplitShardsMetadata metadata = builder.build();
        List<Integer> rootShards = metadata.getRootShards();
        List<Integer> expectedRootShards = List.of(0, 1, 2);
        assertEquals(expectedRootShards, rootShards);

        Set<Integer> expectedActiveShards = Set.of(1, 2, 3, 4, 5);
        Set<Integer> actualShardIds = new HashSet<>();
        for (Iterator<Integer> iterator = metadata.getActiveShardIterator(); iterator.hasNext();) {
            actualShardIds.add(iterator.next());
        }
        assertEquals(expectedActiveShards, actualShardIds);
    }

    /**
     * Test for no split data
     */
    public void testGetActiveShardIterator_emptyIterator() {
        // Arrange
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(0); // maxShardId = -1
        SplitShardsMetadata metadata = builder.build();

        // Act & Assert
        Iterator<Integer> iterator = metadata.getActiveShardIterator();
        assertFalse(iterator.hasNext());
    }

    /**
     * Tests get active shard iterator for split in progress
     */
    public void test_getActiveShardIterator_splitInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);

        Set<Integer> expectedActiveShards = Set.of(0, 1, 2);
        Set<Integer> actualActiveShards = new HashSet<>();
        SplitShardsMetadata metadata = builder.build();
        for (Iterator<Integer> it = metadata.getActiveShardIterator(); it.hasNext();) {
            actualActiveShards.add(it.next());
        }

        // Split in progress, should return all 3 shards
        assertEquals(actualActiveShards, expectedActiveShards);

    }

    /**
     * Tests get active shard iterator for split completed
     */
    public void test_getActiveShardIterator_splitCompleted() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3); // Start with 1 root shard
        // First split - create existing child shards
        builder.splitShard(0, 3);

        // split completed for shard 0
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4, 5));

        Set<Integer> expectedActiveShardsForConsecutiveShardSplit = Set.of(1, 2, 3, 4, 5);
        Set<Integer> actualActiveShardsForConsecutiveShardSplit = new HashSet<>();
        SplitShardsMetadata splitCompletedShardSplitMetadata = builder.build();
        for (Iterator<Integer> it = splitCompletedShardSplitMetadata.getActiveShardIterator(); it.hasNext();) {
            actualActiveShardsForConsecutiveShardSplit.add(it.next());
        }
        // Split in progress, should return all 4 shards
        assertEquals(expectedActiveShardsForConsecutiveShardSplit, actualActiveShardsForConsecutiveShardSplit);
    }

    /**
     * Tests get active shard iterator for consecutive split
     */
    public void test_getActiveShardIterator_consecutiveSplits() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3); // Start with 1 root shard
        // First split - create existing child shards
        builder.splitShard(0, 3);

        Set<Integer> expectedActiveShards = Set.of(0, 1, 2);
        Set<Integer> actualActiveShards = new HashSet<>();
        SplitShardsMetadata metadata = builder.build();
        for (Iterator<Integer> it = metadata.getActiveShardIterator(); it.hasNext();) {
            actualActiveShards.add(it.next());
        }

        // Split in progress, should return all 3 shards
        assertEquals(actualActiveShards, expectedActiveShards);

        // split completed for shard 0
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4, 5));

        // split in progress for shard 1
        builder.splitShard(1, 2);

        // split completed for shard 1
        builder.updateSplitMetadataForChildShards(1, Set.of(6, 7));

        Set<Integer> expectedActiveShardsForConsecutiveShardSplit = Set.of(2, 3, 4, 5, 6, 7);
        Set<Integer> actualActiveShardsForConsecutiveShardSplit = new HashSet<>();
        SplitShardsMetadata consecutiveShardSplitMetadata = builder.build();
        for (Iterator<Integer> it = consecutiveShardSplitMetadata.getActiveShardIterator(); it.hasNext();) {
            actualActiveShardsForConsecutiveShardSplit.add(it.next());
        }

        // Should return only active child shards
        assertEquals(actualActiveShardsForConsecutiveShardSplit, expectedActiveShardsForConsecutiveShardSplit);
    }

    /**
     * Tests getShardIdOfHash when there are existing child shards and in-progress children.
     */
    public void testGetShardIdOfHashWithExistingAndInProgressChildren() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard
        // First split - create existing child shards
        builder.splitShard(0, 3);
        builder.updateSplitMetadataForChildShards(0, Set.of(1, 2, 3));

        // Second split - split the middle shard (ID 2)
        builder.splitShard(2, 2);
        SplitShardsMetadata metadata = builder.build();

        // Execute - test hash that falls in the range of first child of shard 2
        int result = metadata.getShardIdOfHash(0, 500, true);

        // Assert - should route to the first child of the in-progress split
        assertEquals("Hash should route to first child of in-progress split", 5, result);
    }

    /**
     * Test getShardIdOfHash when root shard has no children but in-progress split exists
     */
    public void testGetShardIdOfHashWithInProgressSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard
        // Setup - split root shard
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();

        // Execute - test with hash that should go to second child
        int result = metadata.getShardIdOfHash(0, 100, true);

        // Verify - should route to the second child shard
        assertEquals("Should route to second child shard", 2, result);
    }

    /**
     * Test case for in-progress split being ignored
     */
    public void testGetShardIdOfHashWithInProgressSplitIgnored() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();

        // Should return root shard ID when includeInProgressChildren is false
        assertEquals(0, metadata.getShardIdOfHash(0, 100, false));
    }

    /**
     * Test case for getShardIdOfHash when the root shard has no children and no in-progress split
     */
    public void test_getShardIdOfHash_returnsRootShardId() {
        // Arrange
        int rootShardId = 0;
        int hash = 123;
        boolean includeInProgressChildren = false;

        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // 1 root shard
        SplitShardsMetadata metadata = builder.build();

        // Act
        int result = metadata.getShardIdOfHash(rootShardId, hash, includeInProgressChildren);

        // Assert
        assertEquals("Should return the root shard ID when there are no children", rootShardId, result);
    }

    /**
     * Test getShardIdOfHash when root shard has children but no in-progress splits
     */
    public void test_getShardIdOfHash_withExistingChildren() {
        // Setup
        int rootShardId = 0;
        int hash = 500;
        boolean includeInProgressChildren = true;

        // Setup
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard

        // Split the root shard into 3 pieces
        builder.splitShard(0, 3);

        // Complete the split with three child shards
        Set<Integer> childShardIds = Set.of(1, 2, 3);
        builder.updateSplitMetadataForChildShards(0, childShardIds);

        SplitShardsMetadata metadata = builder.build();

        // Execute
        int result = metadata.getShardIdOfHash(rootShardId, hash, includeInProgressChildren);

        // Verify
        assertEquals(2, result);
    }

    /**
     * Test that getNumberOfRootShards returns the correct number of root shards
     */
    public void testGetNumberOfRootShardsReturnsCorrectCount() {
        // Arrange
        int expectedRootShards = 5;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(expectedRootShards);
        SplitShardsMetadata metadata = builder.build();

        // Act
        int actualRootShards = metadata.getNumberOfRootShards();

        // Assert
        assertEquals("Number of root shards should match the initial count", expectedRootShards, actualRootShards);
    }

    /**
     * Test case for getting number of root shards after splitting
     * Expected behavior: Should return the same number of root shards
     */
    public void testGetNumberOfRootShardsAfterSplitting() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        assertEquals("Number of root shards should remain unchanged after splitting", 5, metadata.getNumberOfRootShards());
    }

    /**
     * Test case for empty metadata
     * Expected behavior: Should return 0 for empty metadata
     */
    public void testGetNumberOfRootShardsWithEmptyMetadata() {
        SplitShardsMetadata emptyMetadata = new SplitShardsMetadata.Builder(0).build();
        assertEquals("Empty metadata should have 0 root shards", 0, emptyMetadata.getNumberOfRootShards());
    }

    /**
     * This test verifies that the getNumberOfShards() method
     * correctly returns maxShardId + 1
     */
    public void testGetNumberOfShardsReturnsMaxShardIdPlusOne() {
        // Arrange
        int maxShardId = 5;
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(maxShardId + 1).build();

        // Act
        int result = metadata.getNumberOfShards();

        // Assert
        assertEquals("getNumberOfShards should return maxShardId + 1", maxShardId + 1, result);
    }

    /**
     * Test case for getting number of shards after splitting
     */
    public void testGetNumberOfShardsAfterSplitting() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        // Split the root shard into 3 pieces
        builder.splitShard(0, 3);

        // Complete the split with three child shards
        Set<Integer> childShardIds = Set.of(1, 2, 3);
        builder.updateSplitMetadataForChildShards(0, childShardIds);
        SplitShardsMetadata metadata = builder.build();
        assertEquals(3, metadata.getNumberOfShards());
    }

    /**
     * Test case for empty metadata
     * Expected behavior: Should return 0 for empty metadata
     */
    public void testGetNumberOfShardsWithEmptyMetadata() {
        SplitShardsMetadata emptyMetadata = new SplitShardsMetadata.Builder(0).build();
        assertEquals("Empty metadata should have 0 shards", 0, emptyMetadata.getNumberOfShards());
    }

    /**
     * Test case for getChildShardsOfParent when the parent shard has child shards
     */
    public void testGetChildShardsOfParentWithExistingChildren() {
        // Create a builder and add some child shards for a parent
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        int parentShardId = 2;
        int numberOfChildren = 3;
        builder.splitShard(parentShardId, numberOfChildren);

        // Build the metadata
        SplitShardsMetadata metadata = builder.build();

        // Get child shards of the parent
        ShardRange[] childShards = metadata.getChildShardsOfParent(parentShardId);

        // Assert that child shards are returned and match the expected number
        assertNotNull(childShards);
        assertEquals(numberOfChildren, childShards.length);

        // Verify that each child shard is a copy and not the original
        ShardRange[] originalChildShards = metadata.getChildShardsOfParent(parentShardId);
        for (int i = 0; i < childShards.length; i++) {
            assertNotSame(originalChildShards[i], childShards[i]);
            assertEquals(originalChildShards[i], childShards[i]);
        }
    }

    /**
     * Test case for getChildShardsOfParent after a cancelled split operation
     */
    public void testGetChildShardsOfParentAfterCancelledSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        builder.cancelSplit(0);
        SplitShardsMetadata metadata = builder.build();

        assertNull("Should return null for shard with cancelled split", metadata.getChildShardsOfParent(0));
    }

    /**
     * Test case for getChildShardsOfParent with an unsplit shard
     */
    public void testGetChildShardsOfParentWithUnsplitShard() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        SplitShardsMetadata metadata = builder.build();

        assertNull("Should return null for unsplit shard", metadata.getChildShardsOfParent(0));
    }

    /**
     * Test case for SplitShardsMetadata.Builder constructor with numberOfShards parameter
     */
    public void testBuilderConstructorWithNumberOfShards() {
        int numberOfShards = 5;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(numberOfShards);

        SplitShardsMetadata metadata = builder.build();

        assertEquals(numberOfShards, metadata.getNumberOfRootShards());
        assertEquals(numberOfShards, metadata.getNumberOfShards());
        assertTrue(metadata.getInProgressSplitShardIds().isEmpty());

        for (int i = 0; i < numberOfShards; i++) {
            assertNull(metadata.getChildShardsOfParent(i));
        }
    }

    /**
     * Tests the Builder constructor with a SplitShardsMetadata instance where some root shards have no children.
     */
    public void testBuilderWithEmptyRootShards() {
        // Create a SplitShardsMetadata with some empty root shards
        SplitShardsMetadata.Builder originalBuilder = new SplitShardsMetadata.Builder(3);
        SplitShardsMetadata original = originalBuilder.build();

        // Create a new Builder using the original SplitShardsMetadata
        SplitShardsMetadata.Builder newBuilder = new SplitShardsMetadata.Builder(original);

        // Build the new SplitShardsMetadata
        SplitShardsMetadata result = newBuilder.build();

        // Assert that the new SplitShardsMetadata matches the original
        assertEquals(original, result);
        assertEquals(3, result.getNumberOfRootShards());
        assertNull(result.getChildShardsOfParent(0));
        assertNull(result.getChildShardsOfParent(1));
        assertNull(result.getChildShardsOfParent(2));
        assertTrue(result.getInProgressSplitShardIds().isEmpty());
    }

    /**
     * Test that cancelSplit correctly removes the in-progress split and resets the inProgressSplitShardId
     */
    public void testCancelSplitRemovesInProgressSplit() {
        // Setup
        int numberOfShards = 5;
        int sourceShardId = 2;
        int numberOfChildren = 2;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(numberOfShards);

        // Start a split
        builder.splitShard(sourceShardId, numberOfChildren);

        // Verify split is in progress
        assertTrue(builder.build().getInProgressSplitShardIds().contains(sourceShardId));

        // Cancel the split
        builder.cancelSplit(sourceShardId);

        // Build the metadata
        SplitShardsMetadata metadata = builder.build();

        // Verify the split was canceled
        assertTrue(metadata.getInProgressSplitShardIds().isEmpty());
        assertNull(metadata.getChildShardsOfParent(sourceShardId));
    }

    /**
     * Test that getInProgressSplitShardId returns the correct shard ID
     */
    public void testGetInProgressSplitShardId() {
        int expectedShardId = 5;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(10);
        builder.splitShard(expectedShardId, 2);
        SplitShardsMetadata metadata = builder.build();

        assertTrue(
            "The in-progress split shard ID should match the expected value",
            metadata.getInProgressSplitShardIds().contains(expectedShardId)
        );
    }

    public void testGetInProgressSplitShardIdAfterCancelingSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        builder.splitShard(0, 2);
        builder.cancelSplit(0);
        SplitShardsMetadata metadata = builder.build();
        assertTrue("After canceling a split, shard split shouldn't be in progress", metadata.getInProgressSplitShardIds().isEmpty());
    }

    public void testGetInProgressSplitShardIdAfterCompletingSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        builder.splitShard(0, 2);
        builder.updateSplitMetadataForChildShards(0, Set.of(1, 2));
        SplitShardsMetadata metadata = builder.build();
        assertTrue("After completing a split, shard split shouldn't be in progress", metadata.getInProgressSplitShardIds().isEmpty());
    }

    public void testGetInProgressSplitShardIdWhenNoSplitInProgress() {
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(1).build();
        assertTrue("No split should be in progress", metadata.getInProgressSplitShardIds().isEmpty());
    }

    public void testGetInProgressSplitShardIdWithValidSplitInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        assertTrue(
            "When a valid split is in progress, the correct shard ID should be returned",
            metadata.getInProgressSplitShardIds().contains(0)
        );
    }

    /**
     * Test that the method returns true when the correct shard is being split.
     */
    public void testIsSplitOfShardInProgress_CorrectShardInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(2, 2);
        SplitShardsMetadata metadata = builder.build();
        assertTrue(metadata.isSplitOfShardInProgress(2));
    }

    /**
     * Test that the method returns false when a different shard is being split.
     */
    public void testIsSplitOfShardInProgress_DifferentShardInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(2, 2);
        SplitShardsMetadata metadata = builder.build();
        assertFalse(metadata.isSplitOfShardInProgress(1));
    }

    /**
     * Test that the method returns false when no split is in progress.
     */
    public void testIsSplitOfShardInProgress_NoSplitInProgress() {
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(5).build();
        assertFalse(metadata.isSplitOfShardInProgress(0));
        assertTrue(metadata.getInProgressSplitShardIds().isEmpty());
    }

    public void testSplitShardWhenSplitAlreadyInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(1, 3);
        metadata = builder.build();
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(2, 5);
        metadata = builder.build();
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4));
        builder.updateSplitMetadataForChildShards(1, Set.of(5, 6, 7));
        builder.updateSplitMetadataForChildShards(2, Set.of(8, 9, 10, 11, 12));
        metadata = builder.build();
        Set<Integer> finalShards = new HashSet<>(Arrays.asList(3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
        Iterator<Integer> shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            assertTrue(finalShards.remove(shardIterator.next()));
        }
        assertTrue(finalShards.isEmpty());
    }

    public void testSplitShardWhenAnotherSplitIsCancelled_1() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(1, 4);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.splitShard(2, 5);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4));
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(2, Set.of(9, 10, 11, 12, 13));
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.cancelSplit(1);
        metadata = builder.build();
        Set<Integer> finalShards = new HashSet<>(Arrays.asList(1, 3, 4, 9, 10, 11, 12, 13));
        Iterator<Integer> shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());

        // Test reusing holes in new split
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(4, 2);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(4, Set.of(5, 6));
        finalShards = new HashSet<>(Arrays.asList(1, 3, 5, 6, 9, 10, 11, 12, 13));
        metadata = builder.build();
        shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());

        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(12, 3);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(12, Set.of(7, 8, 14));
        finalShards = new HashSet<>(Arrays.asList(1, 3, 5, 6, 7, 8, 9, 10, 11, 13, 14));
        metadata = builder.build();
        shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());
    }

    public void testSplitShardWhenAnotherSplitIsCancelled_2() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(1, 3);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.splitShard(2, 5);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4));
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(1, Set.of(5, 6, 7));
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.cancelSplit(2);
        metadata = builder.build();
        Set<Integer> finalShards = new HashSet<>(Arrays.asList(2, 3, 4, 5, 6, 7));
        Iterator<Integer> shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());
    }

    public void testSplitCancelWhenAnotherSplitIsInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(1, 4);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.splitShard(2, 5);
        builder.cancelSplit(1);
        metadata = builder.build();
        Set<Integer> finalShards = new HashSet<>(Arrays.asList(0, 1, 2));
        Iterator<Integer> shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());

        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4));
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(2, Set.of(9, 10, 11, 12, 13));
        builder = new SplitShardsMetadata.Builder(builder.build());
        metadata = builder.build();
        finalShards = new HashSet<>(Arrays.asList(1, 3, 4, 9, 10, 11, 12, 13));
        shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());

        // Test reusing holes in new split
        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(4, 2);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(4, Set.of(5, 6));
        finalShards = new HashSet<>(Arrays.asList(1, 3, 5, 6, 9, 10, 11, 12, 13));
        metadata = builder.build();
        shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());

        builder = new SplitShardsMetadata.Builder(metadata);
        builder.splitShard(12, 3);
        builder = new SplitShardsMetadata.Builder(builder.build());
        builder.updateSplitMetadataForChildShards(12, Set.of(7, 8, 14));
        finalShards = new HashSet<>(Arrays.asList(1, 3, 5, 6, 7, 8, 9, 10, 11, 13, 14));
        metadata = builder.build();
        shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int nextShard = shardIterator.next();
            assertTrue(finalShards.remove(nextShard));
        }
        assertTrue(finalShards.isEmpty());

        // Case 1
        // 1. 0,1,2 original
        // 2. 0 -> 3,4
        // 3. 1 -> 5, 6, 7
        // 4. 0=>X
        // 5. 0 -> 3,4,8
        // 6. 0 completes 1,2,3,4,8 holes is 0, max shard id 8
        // 7. 1=>X, 1,2,3,4,8 hoes is 5,6,7 m=

    }

    public void testChildShardIdsGeneration() {
        int maxInitialShards = 20;
        int initialShards = randomIntBetween(1, maxInitialShards);
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(initialShards);
        SplitShardsMetadata metadata = builder.build();
        int splitExecCount = randomIntBetween(1, 20);
        for (int i = 0; i < splitExecCount; i++) {
            Set<Integer> cancellableShards = new HashSet<>();
            Map<Integer, Set<Integer>> splitCompletingShards = new HashMap<>();
            for (int j = 0; j < 10; j++) {
                List<Integer> activeAndNotInProgress = getActiveAndNotInProgressShards(metadata);
                if (activeAndNotInProgress.isEmpty()) {
                    break;
                }
                builder = new SplitShardsMetadata.Builder(metadata);
                int splittingShard = activeAndNotInProgress.get(randomIntBetween(0, activeAndNotInProgress.size() - 1));

                int numberOfChildren = randomIntBetween(1, 50);
                List<ShardRange> childShardRanges;
                try {
                    childShardRanges = builder.splitShard(splittingShard, numberOfChildren);
                } catch (Exception ex) {
                    if (ex.getMessage().equals("Cannot split shard [" + splittingShard + "] further.")) {
                        break;
                    }
                    throw ex;
                }
                Set<Integer> childShardIds = new HashSet<>();
                childShardRanges.forEach(shardRange -> childShardIds.add(shardRange.getShardId()));
                validateShardsInExistingAndNewMetadata(metadata, builder.build(), new HashSet<>(), -1);
                if (childShardRanges.size() != childShardIds.size()) {
                    builder = new SplitShardsMetadata.Builder(metadata);
                    builder.splitShard(splittingShard, numberOfChildren);
                }
                assertEquals(childShardRanges.size(), childShardIds.size());
                ensureUniqueChildShards(metadata, childShardIds, builder.build());
                metadata = builder.build();

                boolean complete = randomBoolean();
                builder = new SplitShardsMetadata.Builder(metadata);
                boolean cancelSplit = randomBoolean();
                if (complete) {
                    if (cancelSplit) {
                        builder.cancelSplit(splittingShard);
                        validateShardsInExistingAndNewMetadata(metadata, builder.build(), new HashSet<>(), -1);
                        metadata = builder.build();
                        builder = new SplitShardsMetadata.Builder(metadata);
                        builder.splitShard(splittingShard, childShardIds.size());
                        builder.updateSplitMetadataForChildShards(splittingShard, childShardIds);
                        validateShardsInExistingAndNewMetadata(metadata, builder.build(), childShardIds, splittingShard);
                        metadata = builder.build();
                    } else {
                        builder.updateSplitMetadataForChildShards(splittingShard, childShardIds);
                        validateShardsInExistingAndNewMetadata(metadata, builder.build(), childShardIds, splittingShard);
                        metadata = builder.build();
                    }
                } else {
                    if (cancelSplit) {
                        cancellableShards.add(splittingShard);
                    } else {
                        splitCompletingShards.put(splittingShard, childShardIds);
                    }
                }
            }
            finishPendingSplits(cancellableShards, splitCompletingShards, metadata);
        }
    }

    private void finishPendingSplits(
        Set<Integer> cancellableShards,
        Map<Integer, Set<Integer>> splitCompletingShards,
        SplitShardsMetadata metadata
    ) {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(metadata);
        for (Integer shardId : cancellableShards) {
            builder.cancelSplit(shardId);
            validateShardsInExistingAndNewMetadata(metadata, builder.build(), new HashSet<>(), -1);
            metadata = builder.build();
        }

        for (Integer shardId : splitCompletingShards.keySet()) {
            builder = new SplitShardsMetadata.Builder(metadata);
            builder.updateSplitMetadataForChildShards(shardId, splitCompletingShards.get(shardId));
            validateShardsInExistingAndNewMetadata(metadata, builder.build(), splitCompletingShards.get(shardId), shardId);
            metadata = builder.build();
        }
    }

    private List<Integer> getActiveAndNotInProgressShards(SplitShardsMetadata metadata) {
        List<Integer> result = new ArrayList<>();
        Iterator<Integer> shardIterator = metadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            int shardId = shardIterator.next();
            if (metadata.getInProgressSplitShardIds().contains(shardId) == false) {
                result.add(shardId);
            }
        }
        return result;
    }

    private void ensureUniqueChildShards(SplitShardsMetadata previousMetadata, Set<Integer> childShards, SplitShardsMetadata newMetadata) {
        Set<Integer> existingShards = new HashSet<>();
        Iterator<Integer> shardIterator = previousMetadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            existingShards.add(shardIterator.next());
        }
        childShards.forEach(shard -> assertFalse(existingShards.contains(shard)));

        previousMetadata.getInProgressSplitShardIds().forEach(shardId -> assertFalse(childShards.contains(shardId)));
    }

    private void validateShardsInExistingAndNewMetadata(
        SplitShardsMetadata existing,
        SplitShardsMetadata newMetadata,
        Set<Integer> newShards,
        int splittingShard
    ) {
        Set<Integer> existingShards = new HashSet<>();
        Iterator<Integer> shardIterator = existing.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            existingShards.add(shardIterator.next());
        }
        existingShards.remove(splittingShard);

        Set<Integer> shardsFromNewMetadata = new HashSet<>();
        shardIterator = newMetadata.getActiveShardIterator();
        while (shardIterator.hasNext()) {
            shardsFromNewMetadata.add(shardIterator.next());
        }

        assertTrue(shardsFromNewMetadata.containsAll(existingShards));
        if (newShards.isEmpty()) {
            assertEquals(existingShards.size(), shardsFromNewMetadata.size());
        } else {
            assertTrue(shardsFromNewMetadata.containsAll(newShards));
            assertEquals(existingShards.size() + newShards.size(), shardsFromNewMetadata.size());
            assertFalse(newMetadata.getInProgressSplitShardIds().contains(splittingShard));
            ShardRange[] shardRanges = newMetadata.getChildShardsOfParent(splittingShard);
            assertEquals(shardRanges.length, newShards.size());
            for (ShardRange shardRange : shardRanges) {
                assertTrue(newShards.contains(shardRange.getShardId()));
            }
        }
    }

    public void testSplitShardWithCompletedSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        builder.updateSplitMetadataForChildShards(0, Set.of(5, 6));
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> { builder.splitShard(0, 2); });
        assertTrue(exception.getMessage().startsWith("Split of shard [0] is already in progress or completed."));
    }

    public void testSplitShardWithInvalidNumberOfChildren() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> { builder.splitShard(0, 10000000); });
        assertTrue(exception.getMessage().contains("Cannot split shard [0] further."));
    }

    public void testSplitInvalidShardId() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> builder.splitShard(10, 2) // Invalid shard ID
        );
        assertTrue(exception.getMessage().contains("Shard ID doesn't exist in the current list of shard ranges"));
    }

    public void testSplitShardBasicOperation() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        // Test basic split operation
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();

        // Verify split state
        assertTrue(metadata.isSplitOfShardInProgress(0));
        ShardRange[] childShards = metadata.getChildShardsOfParent(0);
        assertNotNull(childShards);
        assertEquals(2, childShards.length);

        // Verify shard ranges
        assertTrue(childShards[0].getEnd() < childShards[1].getStart());
        assertEquals(childShards[0].getEnd() + 1, childShards[1].getStart());
    }

    public void testSplitShardNestedSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard

        // First split - split root shard 0 into two children (shards 1 and 2)
        builder.splitShard(0, 2);
        builder.updateSplitMetadataForChildShards(0, Set.of(1, 2));

        // Verify first split
        SplitShardsMetadata metadata = builder.build();
        ShardRange[] firstLevelShards = metadata.getChildShardsOfParent(0);
        assertNotNull("First level split should create child shards", firstLevelShards);
        assertEquals("Should have 2 child shards", 2, firstLevelShards.length);

        // Second split - split first child shard (shard 1) into three children
        builder.splitShard(1, 3);
        builder.updateSplitMetadataForChildShards(1, Set.of(3, 4, 5));

        // Get final metadata
        metadata = builder.build();

        // Verify the nested split results
        ShardRange[] secondLevelShards = metadata.getChildShardsOfParent(1);
        assertNotNull("Second level split should create child shards", secondLevelShards);
        assertEquals("Should have 3 child shards", 3, secondLevelShards.length);

        assertTrue("Original shard 0 should be split", metadata.isSplitParent(0));
        assertTrue("Child shard 1 should be split", metadata.isSplitParent(1));
        assertFalse("Child shard 2 should not be split", metadata.isSplitParent(2));

        // Verify ranges are properly distributed
        for (int i = 0; i < secondLevelShards.length - 1; i++) {
            assertTrue("Shard ranges should be in order", secondLevelShards[i].getEnd() < secondLevelShards[i + 1].getStart());
            assertEquals("Shard ranges should be contiguous", secondLevelShards[i].getEnd() + 1, secondLevelShards[i + 1].getStart());
        }

        // Verify range boundaries
        ShardRange parentRange = firstLevelShards[0]; // Range of shard 1
        assertEquals("First child should start at parent's start", parentRange.getStart(), secondLevelShards[0].getStart());
        assertEquals(
            "Last child should end at parent's end",
            parentRange.getEnd(),
            secondLevelShards[secondLevelShards.length - 1].getEnd()
        );

        // Test hash routing through the nested structure
        long childShardRangeDiff = Math.abs((long) parentRange.getEnd() - parentRange.getStart() + 1) / 3;
        int hashInFirstThird = parentRange.getStart() + (int) childShardRangeDiff - 1;

        assertEquals("Hash should route to first child of nested split", 3, metadata.getShardIdOfHash(0, hashInFirstThird, true));
    }

    public void testHashCodeAndEquals() {
        assertEquals(createDummySplitShardsMetadataWithNoRandomisation(), createDummySplitShardsMetadataWithNoRandomisation());
        assertEquals(
            createDummySplitShardsMetadataWithNoRandomisation().hashCode(),
            createDummySplitShardsMetadataWithNoRandomisation().hashCode()
        );
    }

    private SplitShardsMetadata createDummySplitShardsMetadataWithNoRandomisation() {

        int numberOfRootShards = 2;
        ShardRange[][] rootToChildShards = new ShardRange[numberOfRootShards][];
        Map<Integer, ShardRange[]> parentToChildShards = new HashMap<>();
        rootToChildShards[0] = new ShardRange[2];
        rootToChildShards[0][0] = new ShardRange(numberOfRootShards, 0, 100);
        rootToChildShards[0][1] = new ShardRange(numberOfRootShards + 1, 0, 100);

        // put details for already split shard into parent-to-child map
        parentToChildShards.put(0, new ShardRange[] { rootToChildShards[0][0], rootToChildShards[0][1] });

        // put details for splitting shard into parent-to-child map
        int inProgressSplitShardId = 1;
        Set<Integer> inProgressSplitShards = Set.of(0);
        Set<Integer> activeShards = Set.of(0);
        parentToChildShards.put(inProgressSplitShardId, new ShardRange[2]);
        parentToChildShards.get(inProgressSplitShardId)[0] = new ShardRange(numberOfRootShards + 2, 0, 100);
        parentToChildShards.get(inProgressSplitShardId)[1] = new ShardRange(numberOfRootShards + 3, 101, 200);

        return new SplitShardsMetadata(rootToChildShards, parentToChildShards, inProgressSplitShards, activeShards, 0);
    }

    public void testUpdateSplitMetadataWithInvalidSourceShardId() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        Set<Integer> newChildShardIds = new HashSet<>();
        newChildShardIds.add(1);
        newChildShardIds.add(2);
        assertThrows(IllegalArgumentException.class, () -> { builder.updateSplitMetadataForChildShards(1, newChildShardIds); });
    }

    public void testUpdateSplitMetadataForChildShards_NoSplitInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        List<ShardRange> childShards = builder.splitShard(0, 2);
        builder.cancelSplit(0);

        Set<Integer> newChildShardIds = new HashSet<>();
        childShards.forEach(shard -> newChildShardIds.add(shard.getShardId()));
        assertThrows(AssertionError.class, () -> { builder.updateSplitMetadataForChildShards(0, newChildShardIds); });
    }

    public void testUpdateSplitMetadataForChildShards_InvalidChildShardId() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        Set<Integer> newChildShardIds = new HashSet<>();
        newChildShardIds.add(5);
        newChildShardIds.add(7);

        assertThrows(AssertionError.class, () -> { builder.updateSplitMetadataForChildShards(0, newChildShardIds); });
    }

    public void testUpdateSplitMetadataForChildShards_MismatchedChildShardCount() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        Set<Integer> newChildShardIds = new HashSet<>();
        newChildShardIds.add(5);

        assertThrows(AssertionError.class, () -> { builder.updateSplitMetadataForChildShards(0, newChildShardIds); });
    }

    public void testUpdateSplitMetadataForChildShards_EmptyNewChildShardIds() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        Set<Integer> newChildShardIds = new HashSet<>();

        assertThrows(AssertionError.class, () -> { builder.updateSplitMetadataForChildShards(0, newChildShardIds); });
    }

    /**
     * Test case for updateSplitMetadataForChildShards method
     * Verifies that the method correctly updates the metadata for child shards
     */
    public void testUpdateSplitMetadataForChildShards_Success() {
        // Arrange
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(2);
        int sourceShardId = 0;
        builder.splitShard(sourceShardId, 2);
        Set<Integer> newChildShardIds = new HashSet<>(Arrays.asList(2, 3));

        // Act
        builder.updateSplitMetadataForChildShards(sourceShardId, newChildShardIds);
        SplitShardsMetadata metadata = builder.build();

        // Assert
        assertTrue(metadata.getInProgressSplitShardIds().isEmpty());
        assertEquals(3, metadata.getNumberOfShards());

        ShardRange[] childShards = metadata.getChildShardsOfParent(sourceShardId);
        assertNotNull(childShards);
        assertEquals(2, childShards.length);
        assertTrue(newChildShardIds.contains(childShards[0].getShardId()));
        assertTrue(newChildShardIds.contains(childShards[1].getShardId()));

    }

    public void testReadDiffFromWithEmptyInput() throws IOException {
        StreamInput emptyInput = Mockito.mock(StreamInput.class);
        when(emptyInput.available()).thenReturn(0);
        assertNotNull(SplitShardsMetadata.readDiffFrom(emptyInput));
    }

    public void testEqualsMethod() {
        // Test 1: Same object reference
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(1).build();
        assertTrue("Same object should be equal to itself", metadata.equals(metadata));

        // Test 2: Null comparison
        assertFalse("Object should not be equal to null", metadata.equals(null));

        // Test 3: Different class
        assertFalse("Should not be equal to different class", metadata.equals(new Object()));

        // Test 4: Equal objects
        SplitShardsMetadata metadata1 = new SplitShardsMetadata.Builder(2).build();
        SplitShardsMetadata metadata2 = new SplitShardsMetadata.Builder(2).build();
        assertTrue("Two equivalent objects should be equal", metadata1.equals(metadata2));

        // Test 5: Different maxShardId
        SplitShardsMetadata differentMaxShardId = new SplitShardsMetadata.Builder(3).build();
        assertFalse("Objects with different maxShardId should not be equal", metadata1.equals(differentMaxShardId));

        // Test 6: Different inProgressSplitShardId
        SplitShardsMetadata.Builder builder1 = new SplitShardsMetadata.Builder(2);
        builder1.splitShard(0, 2);  // This will set inProgressSplitShardId
        SplitShardsMetadata withSplit = builder1.build();
        assertFalse("Objects with different inProgressSplitShardId should not be equal", metadata1.equals(withSplit));

        // Test 7: Different rootShardsToAllChildren
        SplitShardsMetadata.Builder builder2 = new SplitShardsMetadata.Builder(2);
        builder2.splitShard(0, 2);
        builder2.updateSplitMetadataForChildShards(0, Set.of(2, 3));
        SplitShardsMetadata withDifferentRootShards = builder2.build();
        assertFalse("Objects with different rootShardsToAllChildren should not be equal", metadata1.equals(withDifferentRootShards));

        // Test 8: Different parentToChildShards
        SplitShardsMetadata.Builder builder3 = new SplitShardsMetadata.Builder(2);
        builder3.splitShard(1, 2);  // Split different shard
        builder3.updateSplitMetadataForChildShards(1, Set.of(2, 3));
        SplitShardsMetadata withDifferentParentToChild = builder3.build();
        assertFalse(
            "Objects with different parentToChildShards should not be equal",
            withDifferentRootShards.equals(withDifferentParentToChild)
        );

        // Test 9: Symmetric equality
        assertTrue("Equality should be symmetric", metadata1.equals(metadata2) && metadata2.equals(metadata1));

        // Test 10: Transitive equality
        SplitShardsMetadata metadata3 = new SplitShardsMetadata.Builder(2).build();
        assertTrue(
            "Equality should be transitive",
            metadata1.equals(metadata2) && metadata2.equals(metadata3) && metadata1.equals(metadata3)
        );
    }

    public void testEqualsWithNullArrayElements() {
        // Create metadata with null elements in rootShardsToAllChildren
        SplitShardsMetadata.Builder builder1 = new SplitShardsMetadata.Builder(3);
        SplitShardsMetadata metadata1 = builder1.build();

        SplitShardsMetadata.Builder builder2 = new SplitShardsMetadata.Builder(3);
        SplitShardsMetadata metadata2 = builder2.build();

        assertTrue("Objects with null array elements should be equal if structure is same", metadata1.equals(metadata2));
    }

    public void testEqualsWithEmptyMaps() {
        // Create metadata with empty parentToChildShards maps
        SplitShardsMetadata.Builder builder1 = new SplitShardsMetadata.Builder(1);
        SplitShardsMetadata metadata1 = builder1.build();

        SplitShardsMetadata.Builder builder2 = new SplitShardsMetadata.Builder(1);
        SplitShardsMetadata metadata2 = builder2.build();

        assertTrue("Objects with empty maps should be equal if structure is same", metadata1.equals(metadata2));
    }

    public void testValidateShardRangesOverlap() {
        // Test overlapping ranges
        ShardRange[] overlappingRanges = new ShardRange[] {
            new ShardRange(0, Integer.MIN_VALUE, 100),
            new ShardRange(0, 50, Integer.MAX_VALUE)  // Overlaps with previous range at 50-100
        };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> validateShardRanges(0, overlappingRanges));
        assertTrue(exception.getMessage().contains("Shard range overlap"));
    }

    public void testValidateShardRangesBelowThreshold() {
        // Test range below threshold
        ShardRange[] smallRange = new ShardRange[] { new ShardRange(0, Integer.MIN_VALUE, Integer.MIN_VALUE + 10) };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> validateShardRanges(0, smallRange));
        assertTrue(exception.getMessage().contains("below shard range threshold"));
    }

    public void testValidateShardRangesMissingRange() {
        // Test missing range
        ShardRange[] missingRange = new ShardRange[] { new ShardRange(0, Integer.MIN_VALUE, 100), new ShardRange(0, 200, Integer.MAX_VALUE)  // Missing
                                                                                                                                             // range
                                                                                                                                             // at
                                                                                                                                             // 101-199
        };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> validateShardRanges(0, missingRange));
        assertTrue(exception.getMessage().contains("Shard range from 101 to 199 is missing from the list of shard ranges"));
    }

    public void testValidateShardRangesInvalidStartRange() {
        // test shard range not at Integer.Min
        ShardRange[] missingRange = new ShardRange[] {
            new ShardRange(0, Integer.MIN_VALUE + 1, 100),
            new ShardRange(0, 100, Integer.MAX_VALUE)  // Missing range at 101-199
        };
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> validateShardRanges(0, missingRange));
        assertTrue(exception.getMessage().contains("Shard range from -2147483648 to -2147483648 is missing from the list of shard ranges"));

        // Empty shard range
        IllegalArgumentException exception2 = assertThrows(IllegalArgumentException.class, () -> validateShardRanges(0, new ShardRange[0]));
        assertTrue(exception2.getMessage().contains("No shard range defined for child shards of shard 0"));
    }

    public void testStreamSerdeNoSplit() throws IOException {
        SplitShardsMetadata original = new SplitShardsMetadata.Builder(5).build();
        SplitShardsMetadata deserialized = streamRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testStreamSerdeSplitInProgress() throws IOException {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);
        SplitShardsMetadata original = builder.build();
        SplitShardsMetadata deserialized = streamRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testStreamSerdeSplitCompleted() throws IOException {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4, 5));
        SplitShardsMetadata original = builder.build();
        SplitShardsMetadata deserialized = streamRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testStreamSerdeConsecutiveSplits() throws IOException {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4, 5));
        builder.splitShard(3, 2);
        SplitShardsMetadata original = builder.build();
        SplitShardsMetadata deserialized = streamRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testXContentSerdeNoSplit() throws IOException {
        SplitShardsMetadata original = new SplitShardsMetadata.Builder(5).build();
        SplitShardsMetadata deserialized = xContentRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testXContentSerdeSplitInProgress() throws IOException {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);
        SplitShardsMetadata original = builder.build();
        SplitShardsMetadata deserialized = xContentRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testXContentSerdeSplitCompleted() throws IOException {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4, 5));
        SplitShardsMetadata original = builder.build();
        SplitShardsMetadata deserialized = xContentRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testXContentSerdeConsecutiveSplits() throws IOException {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
        builder.splitShard(0, 3);
        builder.updateSplitMetadataForChildShards(0, Set.of(3, 4, 5));
        builder.splitShard(3, 2);
        SplitShardsMetadata original = builder.build();
        SplitShardsMetadata deserialized = xContentRoundTrip(original);
        assertEquals(original, deserialized);
    }

    public void testDiffSerde() throws IOException {
        SplitShardsMetadata before = new SplitShardsMetadata.Builder(3).build();
        SplitShardsMetadata.Builder afterBuilder = new SplitShardsMetadata.Builder(3);
        afterBuilder.splitShard(0, 3);
        SplitShardsMetadata after = afterBuilder.build();

        Diff<SplitShardsMetadata> diff = after.diff(before);
        BytesStreamOutput out = new BytesStreamOutput();
        diff.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            Diff<SplitShardsMetadata> deserializedDiff = SplitShardsMetadata.readDiffFrom(in);
            SplitShardsMetadata applied = deserializedDiff.apply(before);
            assertEquals(after, applied);
        }
    }

    private SplitShardsMetadata streamRoundTrip(SplitShardsMetadata original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            return new SplitShardsMetadata(in);
        }
    }

    private SplitShardsMetadata xContentRoundTrip(SplitShardsMetadata original) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            return SplitShardsMetadata.parse(parser);
        }
    }

}
