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
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
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

/**
 * Test class for {@link RemoteStoreUploaderService}.
 * Tests various scenarios of segment upload functionality including success cases,
 * error handling, and different directory configurations.
 */
public class RemoteStoreUploaderServiceTests extends OpenSearchTestCase {

    /** Mock IndexShard instance used across tests */
    private IndexShard mockIndexShard;

    /** Mock Directory representing the local store directory */
    private Directory mockStoreDirectory;

    /** Mock RemoteSegmentStoreDirectory for remote storage operations */
    private RemoteSegmentStoreDirectory mockRemoteDirectory;

    /** The service under test */
    private RemoteStoreUploaderService uploaderService;

    /** Mock upload listener for tracking upload events */
    private UploadListener mockUploadListener;

    /** Mock function that creates upload listeners */
    private Function<Map<String, Long>, UploadListener> mockUploadListenerFunction;

    /**
     * Sets up the test environment before each test method.
     * Initializes all mock objects and the service under test.
     *
     * @throws Exception if setup fails
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockIndexShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 0);
        when(mockIndexShard.shardId()).thenReturn(shardId);

        // Mock IndexShard methods instead of setting private fields
        when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
        mockStoreDirectory = mock(FilterDirectory.class);
        // Use a real instance with mocked dependencies instead of mocking the final class
        RemoteDirectory remoteDataDirectory = mock(RemoteDirectory.class);
        mockRemoteDirectory = createMockRemoteDirectory(remoteDataDirectory);
        mockUploadListener = mock(UploadListener.class);
        mockUploadListenerFunction = mock(Function.class);

        when(mockUploadListenerFunction.apply(any())).thenReturn(mockUploadListener);

        uploaderService = new RemoteStoreUploaderService(mockIndexShard, mockStoreDirectory, mockRemoteDirectory);
    }

    /**
     * Creates a real RemoteSegmentStoreDirectory instance with mocked dependencies
     * instead of trying to mock the final class directly.
     * This approach is used because RemoteSegmentStoreDirectory is a final class
     * that cannot be mocked directly.
     *
     * @param remoteDirectory the remote directory to use (currently unused)
     * @return a new RemoteSegmentStoreDirectory instance with mocked dependencies
     * @throws RuntimeException if the directory creation fails
     */
    private RemoteSegmentStoreDirectory createMockRemoteDirectory(RemoteDirectory remoteDirectory) {
        try {
            RemoteDirectory remoteDataDirectory = mock(RemoteDirectory.class);
            RemoteDirectory remoteMetadataDirectory = mock(RemoteDirectory.class);
            RemoteStoreLockManager lockManager = mock(RemoteStoreLockManager.class);
            ThreadPool threadPool = mock(ThreadPool.class);
            ShardId shardId = new ShardId(new Index("test", "test"), 0);

            return new RemoteSegmentStoreDirectory(
                remoteDataDirectory,
                remoteMetadataDirectory,
                lockManager,
                threadPool,
                shardId,
                new HashMap<>()
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to create mock RemoteSegmentStoreDirectory", e);
        }
    }

    /**
     * Tests that uploading an empty collection of segments completes successfully
     * without performing any actual upload operations.
     *
     * @throws Exception if the test fails
     */
    public void testUploadSegmentsWithEmptyCollection() throws Exception {
        Collection<String> emptySegments = Collections.emptyList();
        Map<String, Long> segmentSizeMap = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<Void> listener = ActionListener.wrap(
            response -> latch.countDown(),
            exception -> fail("Should not fail for empty segments")
        );

        uploaderService.uploadSegments(emptySegments, segmentSizeMap, listener, mockUploadListenerFunction, false);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    /**
     * Tests successful segment upload with low priority upload flag set to false.
     * Verifies that segments are uploaded correctly and upload listeners are notified.
     *
     * @throws Exception if the test fails
     */
    public void testUploadSegmentsSuccessWithHighPriorityUpload() throws Exception {
        Collection<String> segments = Arrays.asList("segment1", "segment2");
        Map<String, Long> segmentSizeMap = new HashMap<>();
        segmentSizeMap.put("segment1", 100L);
        segmentSizeMap.put("segment2", 200L);

        // Create a fresh mock IndexShard
        IndexShard freshMockShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 1);
        when(freshMockShard.shardId()).thenReturn(shardId);
        when(freshMockShard.state()).thenReturn(IndexShardState.STARTED);

        // Create a mock directory structure that matches what the code expects
        Directory innerMockDelegate = mock(Directory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerMockDelegate));

        FilterDirectory outerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerFilterDirectory));

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDirectory = mock(RemoteDirectory.class);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDirectory,
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            freshMockShard.getThreadPool(),
            freshMockShard.shardId(),
            new HashMap<>()
        );

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            remoteSegmentStoreDirectory
        );

        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onResponse(null);
            return true;
        }).when(remoteDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<Void> listener = ActionListener.wrap(
            response -> latch.countDown(),
            exception -> fail("Upload should succeed: " + exception.getMessage())
        );

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // Verify the upload listener was called correctly
        verify(mockUploadListener, times(2)).beforeUpload(any(String.class));
        verify(mockUploadListener, times(2)).onSuccess(any(String.class));
    }

    /**
     * Tests successful segment upload with low priority upload flag set to true.
     * Verifies that segments are uploaded correctly and upload listeners are notified.
     *
     * @throws Exception if the test fails
     */
    public void testUploadSegmentsSuccessWithLowPriorityUpload() throws Exception {
        Collection<String> segments = Arrays.asList("segment1", "segment2");
        Map<String, Long> segmentSizeMap = new HashMap<>();
        segmentSizeMap.put("segment1", 100L);
        segmentSizeMap.put("segment2", 200L);

        // Create a fresh mock IndexShard
        IndexShard freshMockShard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test", "test"), 1);
        when(freshMockShard.shardId()).thenReturn(shardId);
        when(freshMockShard.state()).thenReturn(IndexShardState.STARTED);

        // Create a mock directory structure that matches what the code expects
        Directory innerMockDelegate = mock(Directory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerMockDelegate));

        FilterDirectory outerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerFilterDirectory));

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDirectory = mock(RemoteDirectory.class);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDirectory,
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            freshMockShard.getThreadPool(),
            freshMockShard.shardId(),
            new HashMap<>()
        );

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            remoteSegmentStoreDirectory
        );

        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onResponse(null);
            return true;
        }).when(remoteDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<Void> listener = ActionListener.wrap(
            response -> latch.countDown(),
            exception -> fail("Upload should succeed: " + exception.getMessage())
        );

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction, true);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // Verify the upload listener was called correctly
        verify(mockUploadListener, times(2)).beforeUpload(any(String.class));
        verify(mockUploadListener, times(2)).onSuccess(any(String.class));
    }

    /**
     * Tests segment upload functionality when using a CompositeDirectory.
     * Verifies that the afterSyncToRemote callback is invoked on the CompositeDirectory
     * after successful upload operations.
     *
     * @throws Exception if the test fails
     */
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
        freshMockShard.shardRouting = mockShardRouting;
        when(mockShardRouting.primary()).thenReturn(true);

        CompositeDirectory mockCompositeDirectory = mock(CompositeDirectory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(mockCompositeDirectory);
        FilterDirectory outerFilterDirectory = new TestFilterDirectory(innerFilterDirectory);

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDirectory = mock(RemoteDirectory.class);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDirectory,
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            freshMockShard.getThreadPool(),
            freshMockShard.shardId(),
            new HashMap<>()
        );

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            remoteSegmentStoreDirectory
        );

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onResponse(null);
            return true;
        }).when(remoteDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<Void> listener = ActionListener.wrap(
            response -> latch.countDown(),
            exception -> fail("Upload should succeed: " + exception.getMessage())
        );

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(mockCompositeDirectory).afterSyncToRemote("segment1");
    }

    /**
     * Tests error handling when a CorruptIndexException occurs during segment upload.
     * Verifies that the shard is failed with the appropriate error message
     * and the upload listener is notified of the failure.
     *
     * @throws Exception if the test fails
     */
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
        freshMockShard.shardRouting = mockShardRouting;
        when(mockShardRouting.primary()).thenReturn(true);

        Directory innerMockDelegate = mock(Directory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerMockDelegate));

        FilterDirectory outerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerFilterDirectory));

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDirectory = mock(RemoteDirectory.class);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDirectory,
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            freshMockShard.getThreadPool(),
            freshMockShard.shardId(),
            new HashMap<>()
        );

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            remoteSegmentStoreDirectory
        );

        CorruptIndexException corruptException = new CorruptIndexException("Index corrupted", "test");
        CountDownLatch latch = new CountDownLatch(1);

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDataDirectory = mock(RemoteDirectory.class);
        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onFailure(corruptException);
            return true;
        }).when(remoteDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        ActionListener<Void> listener = ActionListener.wrap(response -> fail("Should not succeed with corrupt index"), exception -> {
            assertEquals(corruptException, exception);
            latch.countDown();
        });

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(freshMockShard).failShard(eq("Index corrupted (resource=test)"), eq(corruptException));
        verify(mockUploadListener).onFailure("segment1");
    }

    /**
     * Tests error handling when a generic RuntimeException occurs during segment upload.
     * Verifies that the shard is NOT failed (unlike CorruptIndexException)
     * but the upload listener is still notified of the failure.
     *
     * @throws Exception if the test fails
     */
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
        freshMockShard.shardRouting = mockShardRouting;
        when(mockShardRouting.primary()).thenReturn(true);

        Directory innerMockDelegate = mock(Directory.class);
        FilterDirectory innerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerMockDelegate));

        FilterDirectory outerFilterDirectory = new TestFilterDirectory(new TestFilterDirectory(innerFilterDirectory));

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        RemoteDirectory remoteDirectory = mock(RemoteDirectory.class);
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDirectory,
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            freshMockShard.getThreadPool(),
            freshMockShard.shardId(),
            new HashMap<>()
        );

        // Create a new uploader service with the fresh mocks
        RemoteStoreUploaderService testUploaderService = new RemoteStoreUploaderService(
            freshMockShard,
            outerFilterDirectory,
            remoteSegmentStoreDirectory
        );

        RuntimeException genericException = new RuntimeException("Generic error");
        CountDownLatch latch = new CountDownLatch(1);

        // Setup the real RemoteSegmentStoreDirectory to handle copyFrom calls
        doAnswer(invocation -> {
            ActionListener<Void> callback = invocation.getArgument(5);
            callback.onFailure(genericException);
            return true;
        }).when(remoteDirectory).copyFrom(any(), any(), any(), any(), any(), any(), any(Boolean.class));

        ActionListener<Void> listener = ActionListener.wrap(response -> fail("Should not succeed with generic exception"), exception -> {
            assertEquals(genericException, exception);
            latch.countDown();
        });

        testUploaderService.uploadSegments(segments, segmentSizeMap, listener, mockUploadListenerFunction, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        verify(freshMockShard, never()).failShard(any(), any());
        verify(mockUploadListener).onFailure("segment1");
    }

    /**
     * Test implementation of FilterDirectory used for creating nested directory structures
     * in tests. This class simply delegates all operations to the wrapped directory.
     */
    public static class TestFilterDirectory extends FilterDirectory {

        /**
         * Creates a new TestFilterDirectory wrapping the given directory.
         *
         * @param in the directory to wrap
         */
        public TestFilterDirectory(Directory in) {
            super(in);
        }
    }
}
