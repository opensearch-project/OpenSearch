/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteStoreUploaderServiceTests extends OpenSearchTestCase {

    /**
     * Helper method to access private fields using reflection
     */
    @SuppressWarnings("unchecked")
    private <T> T getFieldValue(Object object, String fieldName) {
        try {
            java.lang.reflect.Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get field value: " + fieldName, e);
        }
    }

    private IndexShard mockIndexShard;
    private Directory mockStoreDirectory;
    private RemoteSegmentStoreDirectory mockRemoteDirectory;
    private RemoteStoreUploaderService uploaderService;
    private UploadListener mockUploadListener;
    private Function<Map<String, Long>, UploadListener> mockUploadListenerFunction;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockIndexShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 0);
        when(mockIndexShard.shardId()).thenReturn(shardId);

        // Create a mock ShardRouting and set it as a field on the IndexShard mock
        ShardRouting mockShardRouting = mock(ShardRouting.class);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            shardRoutingField.set(mockIndexShard, mockShardRouting);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set shardRouting field", e);
        }
        mockStoreDirectory = mock(FilterDirectory.class);
        // Use a real instance with mocked dependencies instead of mocking the final class
        mockRemoteDirectory = createMockRemoteDirectory();
        mockUploadListener = mock(UploadListener.class);
        mockUploadListenerFunction = mock(Function.class);

        when(mockUploadListenerFunction.apply(any())).thenReturn(mockUploadListener);

        uploaderService = new RemoteStoreUploaderService(mockIndexShard, mockStoreDirectory, mockRemoteDirectory);
    }

    /**
     * Creates a real RemoteSegmentStoreDirectory instance with mocked dependencies
     * instead of trying to mock the final class directly.
     */
    private RemoteSegmentStoreDirectory createMockRemoteDirectory() {
        try {
            RemoteDirectory remoteDataDirectory = mock(RemoteDirectory.class);
            RemoteDirectory remoteMetadataDirectory = mock(RemoteDirectory.class);
            RemoteStoreLockManager lockManager = mock(RemoteStoreLockManager.class);
            ThreadPool threadPool = mock(ThreadPool.class);
            ShardId shardId = new ShardId(new Index("test", "test"), 0);

            return new RemoteSegmentStoreDirectory(remoteDataDirectory, remoteMetadataDirectory, lockManager, threadPool, shardId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create mock RemoteSegmentStoreDirectory", e);
        }
    }

    public void testUploadSegmentsWithEmptyCollection() throws Exception {
        Collection<String> emptySegments = Collections.emptyList();
        Map<String, Long> segmentSizeMap = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<Void> listener = ActionListener.wrap(
            response -> latch.countDown(),
            exception -> fail("Should not fail for empty segments")
        );

        uploaderService.uploadSegments(emptySegments, segmentSizeMap, listener, mockUploadListenerFunction);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    public void testUploadSegmentsSuccess() throws Exception {
        Collection<String> segments = Arrays.asList("segment1", "segment2");
        Map<String, Long> segmentSizeMap = new HashMap<>();
        segmentSizeMap.put("segment1", 100L);
        segmentSizeMap.put("segment2", 200L);

        // Create a fresh mock IndexShard
        IndexShard freshMockShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 0);
        when(freshMockShard.shardId()).thenReturn(shardId);

        // Create a mock ShardRouting and set it as a field on the IndexShard mock
        ShardRouting mockShardRouting = mock(ShardRouting.class);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            shardRoutingField.set(freshMockShard, mockShardRouting);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set shardRouting field", e);
        }

        // Create a mock directory structure that matches what the code expects
        Directory innerMockDelegate = mock(Directory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerMockDelegate));

        FilterDirectory outerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerFilterDirectory));

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            mockRemoteDirectory
        );

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDataDirectory = getFieldValue(mockRemoteDirectory, "remoteDataDirectory");
        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onResponse(null);
            return true;
        }).when(remoteDataDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<Void> listener = ActionListener.wrap(
            response -> latch.countDown(),
            exception -> fail("Upload should succeed: " + exception.getMessage())
        );

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // Verify the upload listener was called correctly
        verify(mockUploadListener, times(2)).beforeUpload(any(String.class));
        verify(mockUploadListener, times(2)).onSuccess(any(String.class));
    }

    public void testUploadSegmentsWithCompositeDirectory() throws Exception {
        Collection<String> segments = Arrays.asList("segment1");
        Map<String, Long> segmentSizeMap = new HashMap<>();
        segmentSizeMap.put("segment1", 100L);

        // Create a fresh mock IndexShard
        IndexShard freshMockShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 0);
        when(freshMockShard.shardId()).thenReturn(shardId);

        // Create a mock ShardRouting and set it as a field on the IndexShard mock
        ShardRouting mockShardRouting = mock(ShardRouting.class);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            shardRoutingField.set(freshMockShard, mockShardRouting);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set shardRouting field", e);
        }

        CompositeDirectory mockCompositeDirectory = mock(CompositeDirectory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(mockCompositeDirectory);
        FilterDirectory outerFilterDirectory = new TestFilterDirectory(innerFilterDirectory);

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            mockRemoteDirectory
        );

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDataDirectory = getFieldValue(mockRemoteDirectory, "remoteDataDirectory");
        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onResponse(null);
            return true;
        }).when(remoteDataDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<Void> listener = ActionListener.wrap(
            response -> latch.countDown(),
            exception -> fail("Upload should succeed: " + exception.getMessage())
        );

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(mockCompositeDirectory).afterSyncToRemote("segment1");
    }

    public void testUploadSegmentsWithCorruptIndexException() throws Exception {
        Collection<String> segments = Arrays.asList("segment1");
        Map<String, Long> segmentSizeMap = new HashMap<>();
        segmentSizeMap.put("segment1", 100L);

        // Create a fresh mock IndexShard
        IndexShard freshMockShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 0);
        when(freshMockShard.shardId()).thenReturn(shardId);

        // Create a mock ShardRouting and set it as a field on the IndexShard mock
        ShardRouting mockShardRouting = mock(ShardRouting.class);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            shardRoutingField.set(freshMockShard, mockShardRouting);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set shardRouting field", e);
        }

        Directory innerMockDelegate = mock(Directory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerMockDelegate));

        FilterDirectory outerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerFilterDirectory));

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            mockRemoteDirectory
        );

        CorruptIndexException corruptException = new CorruptIndexException("Index corrupted", "test");
        CountDownLatch latch = new CountDownLatch(1);

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDataDirectory = getFieldValue(mockRemoteDirectory, "remoteDataDirectory");
        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onFailure(corruptException);
            return true;
        }).when(remoteDataDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        ActionListener<Void> listener = ActionListener.wrap(response -> fail("Should not succeed with corrupt index"), exception -> {
            assertEquals(corruptException, exception);
            latch.countDown();
        });

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(freshMockShard).failShard(eq("Index corrupted (resource=test)"), eq(corruptException));
        verify(mockUploadListener).onFailure("segment1");
    }

    public void testUploadSegmentsWithGenericException() throws Exception {
        Collection<String> segments = Arrays.asList("segment1");
        Map<String, Long> segmentSizeMap = new HashMap<>();
        segmentSizeMap.put("segment1", 100L);

        // Create a fresh mock IndexShard
        IndexShard freshMockShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 0);
        when(freshMockShard.shardId()).thenReturn(shardId);

        // Create a mock ShardRouting and set it as a field on the IndexShard mock
        ShardRouting mockShardRouting = mock(ShardRouting.class);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            shardRoutingField.set(freshMockShard, mockShardRouting);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set shardRouting field", e);
        }

        Directory innerMockDelegate = mock(Directory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerMockDelegate));

        FilterDirectory outerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerFilterDirectory));

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            mockRemoteDirectory
        );

        RuntimeException genericException = new RuntimeException("Generic error");
        CountDownLatch latch = new CountDownLatch(1);

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDataDirectory = getFieldValue(mockRemoteDirectory, "remoteDataDirectory");
        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onFailure(genericException);
            return true;
        }).when(remoteDataDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        ActionListener<Void> listener = ActionListener.wrap(response -> fail("Should not succeed with generic exception"), exception -> {
            assertEquals(genericException, exception);
            latch.countDown();
        });

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(freshMockShard, never()).failShard(any(), any());
        verify(mockUploadListener).onFailure("segment1");
    }

    public void testIsLowPriorityUpload() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.RECOVERING);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            ShardRouting mockShardRouting = (ShardRouting) shardRoutingField.get(mockIndexShard);
            when(mockShardRouting.primary()).thenReturn(true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access shardRouting field", e);
        }

        RecoveryState mockRecoveryState = mock(RecoveryState.class);
        RecoverySource mockRecoverySource = mock(RecoverySource.class);
        when(mockRecoverySource.getType()).thenReturn(RecoverySource.Type.LOCAL_SHARDS);
        when(mockRecoveryState.getRecoverySource()).thenReturn(mockRecoverySource);
        when(mockIndexShard.recoveryState()).thenReturn(mockRecoveryState);

        assertTrue(uploaderService.isLowPriorityUpload());
    }

    public void testIsLocalOrSnapshotRecoveryOrSeedingWithLocalShards() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.RECOVERING);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            ShardRouting mockShardRouting = (ShardRouting) shardRoutingField.get(mockIndexShard);
            when(mockShardRouting.primary()).thenReturn(true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access shardRouting field", e);
        }

        RecoveryState mockRecoveryState = mock(RecoveryState.class);
        RecoverySource mockRecoverySource = mock(RecoverySource.class);
        when(mockRecoverySource.getType()).thenReturn(RecoverySource.Type.LOCAL_SHARDS);
        when(mockRecoveryState.getRecoverySource()).thenReturn(mockRecoverySource);
        when(mockIndexShard.recoveryState()).thenReturn(mockRecoveryState);

        assertTrue(uploaderService.isLocalOrSnapshotRecoveryOrSeeding());
    }

    public void testIsLocalOrSnapshotRecoveryOrSeedingWithSnapshot() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.RECOVERING);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            ShardRouting mockShardRouting = (ShardRouting) shardRoutingField.get(mockIndexShard);
            when(mockShardRouting.primary()).thenReturn(true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access shardRouting field", e);
        }

        RecoveryState mockRecoveryState = mock(RecoveryState.class);
        RecoverySource mockRecoverySource = mock(RecoverySource.class);
        when(mockRecoverySource.getType()).thenReturn(RecoverySource.Type.SNAPSHOT);
        when(mockRecoveryState.getRecoverySource()).thenReturn(mockRecoverySource);
        when(mockIndexShard.recoveryState()).thenReturn(mockRecoveryState);

        assertTrue(uploaderService.isLocalOrSnapshotRecoveryOrSeeding());
    }

    public void testIsLocalOrSnapshotRecoveryOrSeedingWithSeeding() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.RECOVERING);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            ShardRouting mockShardRouting = (ShardRouting) shardRoutingField.get(mockIndexShard);
            when(mockShardRouting.primary()).thenReturn(true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access shardRouting field", e);
        }

        RecoveryState mockRecoveryState = mock(RecoveryState.class);
        RecoverySource mockRecoverySource = mock(RecoverySource.class);
        when(mockRecoverySource.getType()).thenReturn(RecoverySource.Type.PEER);
        when(mockRecoveryState.getRecoverySource()).thenReturn(mockRecoverySource);
        when(mockIndexShard.recoveryState()).thenReturn(mockRecoveryState);
        when(mockIndexShard.shouldSeedRemoteStore()).thenReturn(true);

        assertTrue(uploaderService.isLocalOrSnapshotRecoveryOrSeeding());
    }

    public void testIsLocalOrSnapshotRecoveryOrSeedingReturnsFalse() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            ShardRouting mockShardRouting = (ShardRouting) shardRoutingField.get(mockIndexShard);
            when(mockShardRouting.primary()).thenReturn(true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access shardRouting field", e);
        }

        assertFalse(uploaderService.isLocalOrSnapshotRecoveryOrSeeding());
    }

    public void testIsLocalOrSnapshotRecoveryOrSeedingWithNonPrimary() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.RECOVERING);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            ShardRouting mockShardRouting = (ShardRouting) shardRoutingField.get(mockIndexShard);
            when(mockShardRouting.primary()).thenReturn(false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access shardRouting field", e);
        }

        assertFalse(uploaderService.isLocalOrSnapshotRecoveryOrSeeding());
    }

    public void testIsLocalOrSnapshotRecoveryOrSeedingWithNullRecoveryState() {
        when(mockIndexShard.state()).thenReturn(IndexShardState.RECOVERING);
        try {
            java.lang.reflect.Field shardRoutingField = IndexShard.class.getDeclaredField("shardRouting");
            shardRoutingField.setAccessible(true);
            ShardRouting mockShardRouting = (ShardRouting) shardRoutingField.get(mockIndexShard);
            when(mockShardRouting.primary()).thenReturn(true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access shardRouting field", e);
        }
        when(mockIndexShard.recoveryState()).thenReturn(null);

        assertFalse(uploaderService.isLocalOrSnapshotRecoveryOrSeeding());
    }

    public static class TestFilterDirectory extends FilterDirectory {

        /**
         * Sole constructor, typically called from sub-classes.
         *
         * @param in input directory
         */
        public TestFilterDirectory(Directory in) {
            super(in);
        }
    }
}
