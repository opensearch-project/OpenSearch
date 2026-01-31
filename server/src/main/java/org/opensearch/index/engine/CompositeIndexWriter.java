/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.Assertions;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.store.Store;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.opensearch.index.BucketedCompositeDirectory.CHILD_DIRECTORY_PREFIX;

/**
 * <p>
 * InternalEngine delegates all IndexWriter specific operations through the
 * CompositeIndexWriter class rather than directly interacting with IndexWriter.
 * This wrapper serves as a unified interface for coordinating write operations
 * with group-specific IndexWriters and managing read operations through an
 * accumulating parent IndexWriter. This wrapper class also handles synchronization
 * of group-specific IndexWriters with the accumulating IndexWriter during refresh
 * by implementing the RefreshListener interface.
 *
 * <p>
 * In addition to managing group-specific IndexWriters, CompositeIndexWriter tracks
 * all updates and deletions applied during each refresh cycle. This state is maintained
 * using a refresh-rotating map structure analogous to LiveVersionMap's implementation.
 *
 * <p>
 * Indexing
 * <p>
 * During indexing, CompositeIndexWriter evaluates the group for a document using a
 * grouping criteria function. The specific IndexWriter selected for indexing a document
 * depends on the outcome of the document for the grouping criteria function. Should the
 * relevant IndexWriter entry inside the map be null, a new IndexWriter will be instantiated
 * for this criteria and added to the map
 * <p>
 * Version Resolution
 * <p>
 * InternalEngine resolves the current version of a document before indexing it to
 * determine whether the request is an indexing or update operation. InternalEngine
 * performs this by first doing a lookup in the version map. In case no version of the
 * document is present in the version map, it queries Lucene via the searcher to look
 * for the current version of the document. Since the version map is maintained throughout
 * an entire refresh cycle, there is no change in how versions are resolved in the above
 * approach. InternalEngine performs a lookup for the document first in the version map
 * followed by querying the document associated with the parent IndexWriter.
 * <p>
 *
 * Locking Mechanism
 * <p>
 * OpenSearch currently utilizes ReentrantReadWriteLock to ensure the underlying
 * IndexWriter is not closed during active indexing. With context-aware segments, an
 * additional lock is used for each IndexWriterMap inside CompositeIndexWriter.
 * <p>
 *
 * During each write/update/delete operation, a read lock on the ReentrantLock
 * associated with the map is acquired. This lock is released when indexing completes.
 * During refresh, a write lock on the same ReentrantLock is obtained just before
 * rotating the WriterMap. Since the write lock is acquired only when there is no
 * active read lock on the writer, all writers in a map are closed and synced with
 * the parent writer only when there are no active writes happening on these IndexWriters.
 * <p>
 *
 * Updates and Deletes
 * <p>
 * With multiple IndexWriters, indexing and updates can occur on different IndexWriters.
 * Therefore, document versions must be synchronized across IndexWriters. This is achieved
 * by performing a partial soft delete (delete without indexing tombstone entry) on the
 * IndexWriters containing the previous version of the document.
 *
 * @see org.opensearch.index.engine.InternalEngine
 * @see org.apache.lucene.search.ReferenceManager.RefreshListener
 * @see org.opensearch.index.engine.LiveVersionMap
 * @see org.apache.lucene.index.IndexWriter
 */
public class CompositeIndexWriter implements DocumentIndexWriter {

    private final EngineConfig engineConfig;
    private final IndexWriter accumulatingIndexWriter;
    private final CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> childIndexWriterFactory;
    private final NumericDocValuesField softDeletesField;
    protected final Logger logger;
    private volatile AtomicBoolean closed;
    private final SoftDeletesPolicy softDeletesPolicy;
    private final Store store;
    private static final String DUMMY_TOMBSTONE_DOC_ID = "-2";
    private final IndexWriterFactory nativeIndexWriterFactory;
    /**
     * pendingNumDocs is used to track pendingNumDocs for child level IndexWriters. Since pendingNumDocs is incremented
     * (by one) only in DocumentsWriterPerThread#reserveOneDoc for any index or update operation, we keep incrementing
     * pendingNumDocs by one for each of these operations. We increment this value whenever we call following functions
     * on childWriter:
     * - softUpdateDocument
     * - softUpdateDocuments
     * - addDocuments
     * - addDocument
     *
     * This value may overshoot during refresh temporarily due to double counting few documents in both old child
     * IndexWriters and parent which should ok as undershooting pendingNumDocs can be problematic.
     */
    private final AtomicLong childWriterPendingNumDocs = new AtomicLong();

    public CompositeIndexWriter(
        EngineConfig engineConfig,
        IndexWriter accumulatingIndexWriter,
        SoftDeletesPolicy softDeletesPolicy,
        NumericDocValuesField softDeletesField,
        IndexWriterFactory nativeIndexWriterFactory
    ) {
        this.engineConfig = engineConfig;
        this.accumulatingIndexWriter = accumulatingIndexWriter;
        this.softDeletesPolicy = softDeletesPolicy;
        this.childIndexWriterFactory = this::createChildWriterUtil;
        this.softDeletesField = softDeletesField;
        this.store = engineConfig.getStore();
        this.logger = Loggers.getLogger(Engine.class, engineConfig.getShardId());
        this.closed = new AtomicBoolean(false);
        this.nativeIndexWriterFactory = nativeIndexWriterFactory;
    }

    /**
     *
     * All write operations will now be handled by a pool of group specific disposable
     * IndexWriters. These disposable IndexWriters will be modelled after Lucene's DWPTs
     * (DocumentsWriterPerThread).
     *
     * <h2>States of Disposable IndexWriters</h2>
     *
     * <p>Similar to DWPTs, these disposable IndexWriters will have three states:</p>
     *
     * <h3>Active</h3>
     *
     * <p>IndexWriters in this state will handle all write requests coming to InternalEngine.
     * For each group/tenant, there will be at most a single IndexWriter that will be in the
     * active state. OpenSearch maintains a mapping of active IndexWriters, each associated
     * with a specific group. During indexing, the specific IndexWriter selected for indexing
     * a document will depend on the outcome of the document for the grouping criteria function.
     * Should there be no active IndexWriter for a group, a new IndexWriter will be instantiated
     * for this criteria and added to the pool.</p>
     *
     * <h3>Mark for Refresh</h3>
     *
     * <p>During refresh, we transition all group specific active IndexWriters from active pool
     * to an intermediate refresh pending state. At this stage, these IndexWriters will not be
     * accepting any active writes, but will continue to handle any ongoing operation.</p>
     *
     * <h3>Close</h3>
     *
     * <p>At this stage, OpenSearch will sync the content of group specific IndexWriters with
     * an accumulating parent IndexWriter via Lucene's addIndexes API call. Post the sync, we
     * remove all group specific IndexWriters from Mark for refresh stage and close them.</p>
     *
     */
    static class DisposableIndexWriter {

        private final IndexWriter indexWriter;
        private final CriteriaBasedIndexWriterLookup lookupMap;

        public DisposableIndexWriter(IndexWriter indexWriter, CriteriaBasedIndexWriterLookup lookupMap) {
            this.indexWriter = indexWriter;
            this.lookupMap = lookupMap;
        }

        public IndexWriter getIndexWriter() {
            return indexWriter;
        }

        public CriteriaBasedIndexWriterLookup getLookupMap() {
            return lookupMap;
        }
    }

    /**
     * This class represents a lookup entry inside LiveIndexWriterDeletesMap. This class is mapped similar to <code>
     * LiveVersionMap.VersionLookup</code>. This is maintained on per refresh cycle basis. This contains the group
     * specific IndexWriter associated with this refresh cycle, the updates/deletes that came in this refresh cycle and
     * a pair of read/write lock which is used to ensure that a look is correctly closed (no ongoing operation on
     * IndexWriter associated with lookup). Composite IndexWriter syncs a lookup with accumulating IndexWriter during
     * each refresh cycle.
     *
     */
    public static final class CriteriaBasedIndexWriterLookup implements Closeable {
        private final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap;
        private final Map<BytesRef, DeleteEntry> lastDeleteEntrySet;
        private final Map<BytesRef, String> criteria;
        private final ReentrantReadWriteLock mapLock;
        private final CriteriaBasedWriterLock mapReadLock;
        private final ReleasableLock mapWriteLock;
        private final long version;
        private boolean closed;

        private static final CriteriaBasedIndexWriterLookup EMPTY = new CriteriaBasedIndexWriterLookup(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            0
        );

        private CriteriaBasedIndexWriterLookup(
            final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap,
            Map<BytesRef, DeleteEntry> lastDeleteEntrySet,
            Map<BytesRef, String> criteria,
            long version
        ) {
            this.criteriaBasedIndexWriterMap = criteriaBasedIndexWriterMap;
            this.lastDeleteEntrySet = lastDeleteEntrySet;
            this.mapLock = new ReentrantReadWriteLock();
            this.mapReadLock = new CriteriaBasedWriterLock(mapLock.readLock(), this);
            this.mapWriteLock = new ReleasableLock(mapLock.writeLock());
            this.criteria = criteria;
            this.version = version;
            this.closed = false;
        }

        DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(
            String criteria,
            CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier
        ) {
            return criteriaBasedIndexWriterMap.computeIfAbsent(criteria, (key) -> {
                try {
                    return indexWriterSupplier.apply(criteria, this);
                } catch (IOException e) {
                    throw new OpenSearchException(e);
                }
            });
        }

        DisposableIndexWriter getIndexWriterForCriteria(String criteria) {
            return criteriaBasedIndexWriterMap.get(criteria);
        }

        int sizeOfCriteriaBasedIndexWriterMap() {
            return criteriaBasedIndexWriterMap.size();
        }

        int sizeOfLastDeleteEntrySet() {
            return lastDeleteEntrySet.size();
        }

        void putLastDeleteEntry(BytesRef uid, DeleteEntry deleteEntry) {
            lastDeleteEntrySet.put(uid, deleteEntry);
        }

        void putCriteriaForDoc(BytesRef key, String criteria) {
            this.criteria.put(key, criteria);
        }

        String getCriteriaForDoc(BytesRef key) {
            return criteria.get(key);
        }

        void removeLastDeleteEntry(BytesRef key) {
            lastDeleteEntrySet.remove(key);
        }

        CriteriaBasedWriterLock getMapReadLock() {
            return mapReadLock;
        }

        boolean hasNewChanges() {
            return !criteriaBasedIndexWriterMap.isEmpty() || !lastDeleteEntrySet.isEmpty();
        }

        @Override
        public void close() throws IOException {
            this.closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        private static final class CriteriaBasedWriterLock implements Releasable {
            private final Lock lock;
            // a per-thread count indicating how many times the thread has entered the lock; only works if assertions are enabled
            private final ThreadLocal<Integer> holdingThreads;
            private final CriteriaBasedIndexWriterLookup lookup;

            public CriteriaBasedWriterLock(Lock lock, CriteriaBasedIndexWriterLookup lookup) {
                this.lock = lock;
                if (Assertions.ENABLED) {
                    holdingThreads = new ThreadLocal<>();
                } else {
                    holdingThreads = null;
                }

                this.lookup = lookup;
            }

            @Override
            public void close() {
                lock.unlock();
                assert removeCurrentThread();
            }

            public CriteriaBasedIndexWriterLookup acquire() throws EngineException {
                lock.lock();
                assert addCurrentThread();
                return lookup;
            }

            /**
             * Try acquiring lock, returning null if unable.
             */
            public CriteriaBasedIndexWriterLookup tryAcquire() {
                boolean locked = lock.tryLock();
                if (locked) {
                    assert addCurrentThread();
                    if (lookup.isClosed()) {
                        this.close();
                        return null;
                    }

                    return lookup;
                } else {
                    return null;
                }
            }

            /**
             * Try acquiring lock, returning null if unable to acquire lock within timeout.
             */
            public CriteriaBasedIndexWriterLookup tryAcquire(TimeValue timeout) throws InterruptedException {
                boolean locked = lock.tryLock(timeout.duration(), timeout.timeUnit());
                if (locked) {
                    assert addCurrentThread();
                    return lookup;
                } else {
                    return null;
                }
            }

            private boolean addCurrentThread() {
                final Integer current = holdingThreads.get();
                holdingThreads.set(current == null ? 1 : current + 1);
                return true;
            }

            private boolean removeCurrentThread() {
                final Integer count = holdingThreads.get();
                assert count != null && count > 0;
                if (count == 1) {
                    holdingThreads.remove();
                } else {
                    holdingThreads.set(count - 1);
                }
                return true;
            }

            public boolean isHeldByCurrentThread() {
                if (holdingThreads == null) {
                    throw new UnsupportedOperationException("asserts must be enabled");
                }
                final Integer count = holdingThreads.get();
                return count != null && count > 0;
            }
        }
    }

    private static class DeleteEntry {
        private final Term term;
        private final long version;
        private final long seqNo;
        private final long primaryTerm;

        public DeleteEntry(Term term, long version, long seqNo, long primaryTerm) {
            this.term = term;
            this.version = version;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
        }

        public Term getTerm() {
            return term;
        }
    }

    /**
     * Map used for maintaining <code>CriteriaBasedIndexWriterLookup</code>
     *
     * @opensearch.internal
     */
    final static class LiveIndexWriterDeletesMap {
        // All writes (adds and deletes) go into here:
        final CriteriaBasedIndexWriterLookup current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes. We read from both current and old on lookup:
        final CriteriaBasedIndexWriterLookup old;

        LiveIndexWriterDeletesMap(CriteriaBasedIndexWriterLookup current, CriteriaBasedIndexWriterLookup old) {
            this.current = current;
            this.old = old;
        }

        LiveIndexWriterDeletesMap() {
            this(
                new CriteriaBasedIndexWriterLookup(
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(),
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(),
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(),
                    0
                ),
                CriteriaBasedIndexWriterLookup.EMPTY
            );
        }

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        LiveIndexWriterDeletesMap buildTransitionMap() {
            // This ensures writer map is not rotated during the time when we are obtaining an IndexWriter from map. As
            // this may cause updates to go out of sync with current IndexWriter.
            return new LiveIndexWriterDeletesMap(
                new CriteriaBasedIndexWriterLookup(
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfCriteriaBasedIndexWriterMap()),
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfLastDeleteEntrySet()),
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfLastDeleteEntrySet()),
                    current.version + 1
                ),
                current
            );
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         */
        LiveIndexWriterDeletesMap invalidateOldMap() {
            return new LiveIndexWriterDeletesMap(current, CriteriaBasedIndexWriterLookup.EMPTY);
        }

        void putLastDeleteEntryInCurrentMap(BytesRef uid, DeleteEntry deleteEntry) {
            current.putLastDeleteEntry(uid, deleteEntry);
        }

        void putCriteriaForDoc(BytesRef key, String criteria) {
            current.putCriteriaForDoc(key, criteria);
        }

        String getCriteriaForDoc(BytesRef key) {
            return current.getCriteriaForDoc(key);
        }

        DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(
            String criteria,
            CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier,
            ShardId shardId
        ) {
            boolean success = false;
            CriteriaBasedIndexWriterLookup current = null;
            try {
                while (current == null || current.isClosed()) {
                    // This function acquires a first read lock on a map which does not have any write lock present. Current keeps
                    // on getting rotated during refresh, so there will be one current on which read lock can be obtained.
                    // Validate that no write lock is applied on the map and the map is not closed. Idea here is write lock was
                    // never applied on this map as write lock gets only during closing time. We are doing this instead of acquire,
                    // because acquire can also apply a read lock in case refresh completed and map is closed.
                    current = this.current.mapReadLock.tryAcquire();
                }

                DisposableIndexWriter writer = current.computeIndexWriterIfAbsentForCriteria(criteria, indexWriterSupplier);
                success = true;
                return writer;
            } finally {
                if (success == false && current != null) {
                    assert current.mapReadLock.isHeldByCurrentThread() == true;
                    current.mapReadLock.close();
                }
            }
        }

        // Used for Test Case.
        ReleasableLock acquireCurrentWriteLock() {
            return current.mapWriteLock.acquire();
        }

        boolean hasNewIndexingOrUpdates() {
            return current.hasNewChanges() || old.hasNewChanges();
        }
    }

    private volatile LiveIndexWriterDeletesMap liveIndexWriterDeletesMap = new LiveIndexWriterDeletesMap();

    @Override
    public void beforeRefresh() throws IOException {
        // Rotate map first so all new writes goes to new generation writers.
        liveIndexWriterDeletesMap = liveIndexWriterDeletesMap.buildTransitionMap();
        logger.debug("Trying to acquire write lock during refresh of composite IndexWriter. ");
        try (
            Releasable ignore = liveIndexWriterDeletesMap.old.mapWriteLock.acquire();
            CriteriaBasedIndexWriterLookup oldMap = liveIndexWriterDeletesMap.old;
        ) {
            logger.debug("Acquired write lock during refresh of composite IndexWriter.");
            // TODO No more write should happen post this, so that before refresh for syncing writers have all old writers available.
            // TODO Or should we do this in Reader before listner where we are syncing data??
            refreshDocumentsForParentDirectory(oldMap);
        } catch (Throwable ex) {
            rollback();
            throw ex;
        }
    }

    private void refreshDocumentsForParentDirectory(CriteriaBasedIndexWriterLookup oldMap) throws IOException {
        final Map<String, CompositeIndexWriter.DisposableIndexWriter> markForRefreshIndexWritersMap = oldMap.criteriaBasedIndexWriterMap;
        deletePreviousVersionsForUpdatedDocuments();
        Directory directoryToCombine;
        for (CompositeIndexWriter.DisposableIndexWriter childDisposableWriter : markForRefreshIndexWritersMap.values()) {
            directoryToCombine = childDisposableWriter.getIndexWriter().getDirectory();
            childDisposableWriter.getIndexWriter().close();
            long pendingNumDocsByOldChildWriter = childDisposableWriter.getIndexWriter().getPendingNumDocs();
            accumulatingIndexWriter.addIndexes(directoryToCombine);
            Path childDirectoryPath = getLocalFSDirectory(directoryToCombine).getDirectory();
            IOUtils.closeWhileHandlingException(directoryToCombine);
            childWriterPendingNumDocs.addAndGet(-pendingNumDocsByOldChildWriter);
            IOUtils.rm(childDirectoryPath);
        }

        deleteDummyTombstoneEntry();
    }

    private FSDirectory getLocalFSDirectory(Directory localDirectory) {
        // Since we are validating child IndexWriter directory, it will always be instance of FSDirectory.
        assert localDirectory instanceof FSDirectory;
        return (FSDirectory) localDirectory;
    }

    private void deleteDummyTombstoneEntry() throws IOException {
        Term uid = new Term(IdFieldMapper.NAME, DUMMY_TOMBSTONE_DOC_ID);
        accumulatingIndexWriter.deleteDocuments(uid);
    }

    private void deletePreviousVersionsForUpdatedDocuments() throws IOException {
        Map<BytesRef, DeleteEntry> deleteEntrySet = getLastDeleteEntrySet();
        for (DeleteEntry deleteEntry : deleteEntrySet.values()) {
            // For both updates and deletes do a delete only in parent. For updates, latest writes will be on mark for flush writer,
            // do delete entry in parent. For delete, do a delete in parent. This will take care of scenario incase deleteInLucene,
            // delete went to mark for refresh.
            addDeleteEntryToWriter(deleteEntry, accumulatingIndexWriter);
        }
    }

    /**
     * This function is used for performing partial soft delete (delete without inserting a tombstone entry). This is
     * used for maintaining a single version of documents across all IndexWriter in a shard. To do this, we perform a
     * soft delete using a dummy temporary document as a tombstone entry during the soft update call. This dummy document
     * is hard deleted just before refresh.
     *
     * @param deleteEntry
     * @param currentWriter
     * @throws IOException
     */
    private void addDeleteEntryToWriter(DeleteEntry deleteEntry, IndexWriter currentWriter) throws IOException {
        Document document = new Document();
        document.add(new Field("_id", DUMMY_TOMBSTONE_DOC_ID, IdFieldMapper.Defaults.FIELD_TYPE));
        document.add(new NumericDocValuesField(VersionFieldMapper.NAME, deleteEntry.version));
        document.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, deleteEntry.primaryTerm));
        currentWriter.softUpdateDocument(deleteEntry.term, document, softDeletesField);
    }

    public ReleasableLock getOldWriteLock() {
        return liveIndexWriterDeletesMap.old.mapWriteLock;
    }

    public ReleasableLock getNewWriteLock() {
        return liveIndexWriterDeletesMap.current.mapWriteLock;
    }

    // Used for unit tests.
    CriteriaBasedIndexWriterLookup acquireNewReadLock() {
        return liveIndexWriterDeletesMap.current.mapReadLock.acquire();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        liveIndexWriterDeletesMap = liveIndexWriterDeletesMap.invalidateOldMap();
    }

    public Map<BytesRef, DeleteEntry> getLastDeleteEntrySet() {
        return liveIndexWriterDeletesMap.old.lastDeleteEntrySet;
    }

    void putLastDeleteEntryUnderLockInNewMap(BytesRef uid, DeleteEntry entry) {
        liveIndexWriterDeletesMap.putLastDeleteEntryInCurrentMap(uid, entry);
    }

    void putCriteria(BytesRef uid, String criteria) {
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        liveIndexWriterDeletesMap.putCriteriaForDoc(uid, criteria);
    }

    DisposableIndexWriter getIndexWriterForIdFromCurrent(BytesRef uid) {
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        return getIndexWriterForIdFromLookup(uid, liveIndexWriterDeletesMap.current);
    }

    DisposableIndexWriter getIndexWriterForIdFromOld(BytesRef uid) {
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        return getIndexWriterForIdFromLookup(uid, liveIndexWriterDeletesMap.old);
    }

    // Avoid the issue of write lock getting applied on a separate map due to map getting rotated.
    DisposableIndexWriter getIndexWriterForIdFromLookup(BytesRef uid, CriteriaBasedIndexWriterLookup indexWriterLookup) {
        boolean isCriteriaNotNull = false;
        try {
            indexWriterLookup.mapReadLock.acquire();
            String criteria = indexWriterLookup.getCriteriaForDoc(uid);
            if (criteria != null) {
                DisposableIndexWriter disposableIndexWriter = indexWriterLookup.getIndexWriterForCriteria(criteria);
                if (disposableIndexWriter != null) {
                    isCriteriaNotNull = true;
                    return disposableIndexWriter;
                }
            }

            return null;
        } finally {
            if (isCriteriaNotNull == false) {
                indexWriterLookup.mapReadLock.close();
            }
        }
    }

    public boolean hasNewIndexingOrUpdates() {
        return liveIndexWriterDeletesMap.hasNewIndexingOrUpdates();
    }

    public boolean validateImmutableFieldNotUpdated(ParseContext.Document currentDocument, BytesRef currentUID) {
        String currentCriteria = currentDocument.getGroupingCriteria();
        String previousCriteria = getCriteriaForUID(currentUID, liveIndexWriterDeletesMap);
        // previousCriteria may be null in case version is coming from a tombstone. In that case, we should ignore
        // it.
        return previousCriteria != null && previousCriteria.equals(currentCriteria) == false;
    }

    private String getCriteriaForUID(BytesRef uid, LiveIndexWriterDeletesMap currentMap) {
        String criteria = currentMap.current.getCriteriaForDoc(uid);
        if (criteria != null) {
            return criteria;
        }

        return currentMap.old.getCriteriaForDoc(uid);
    }

    DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(
        final String criteria,
        CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier
    ) throws IOException {
        return computeIndexWriterIfAbsentForCriteria(criteria, liveIndexWriterDeletesMap, indexWriterSupplier);
    }

    DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(
        final String criteria,
        LiveIndexWriterDeletesMap currentLiveIndexWriterDeletesMap,
        CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier
    ) {
        return currentLiveIndexWriterDeletesMap.computeIndexWriterIfAbsentForCriteria(
            criteria,
            indexWriterSupplier,
            engineConfig.getShardId()
        );
    }

    public Map<String, DisposableIndexWriter> getMarkForRefreshIndexWriterMap() {
        return liveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap;
    }

    @Override
    public long getFlushingBytes() {
        ensureOpen();
        return getFlushingBytesUtil(liveIndexWriterDeletesMap);
    }

    /**
     * Utility class to calculate flushingBytes. Since we are point in time current instance of
     * LiveIndexWriterDeletesMap, we should not worry about double counting due to map rotation from current to old. Also
     * when old will be synced with parent writer, old writer is closed so we do not need to worry about double counting
     * in parent and old IndexWriters.
     *
     * @param currentLiveIndexWriterDeletesMap current point in time instance of LiveIndexWriterDeletesMap.
     * @return flushingBytes
     */
    public long getFlushingBytesUtil(LiveIndexWriterDeletesMap currentLiveIndexWriterDeletesMap) {
        long flushingBytes = 0;
        for (DisposableIndexWriter disposableIndexWriter : currentLiveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values()) {
            try {
                flushingBytes += disposableIndexWriter.getIndexWriter().getFlushingBytes();
            } catch (AlreadyClosedException e) {
                if (disposableIndexWriter.getIndexWriter().getTragicException() != null) {
                    throw e;
                }
            }
        }

        for (DisposableIndexWriter disposableIndexWriter : currentLiveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap.values()) {
            try {
                flushingBytes += disposableIndexWriter.getIndexWriter().getFlushingBytes();
            } catch (AlreadyClosedException e) {
                if (disposableIndexWriter.getIndexWriter().getTragicException() != null) {
                    throw e;
                }
            }
        }

        return flushingBytes + accumulatingIndexWriter.getFlushingBytes();
    }

    @Override
    public long getPendingNumDocs() {
        ensureOpen();
        return childWriterPendingNumDocs.get() + accumulatingIndexWriter.getPendingNumDocs();
    }

    @Override
    public LiveIndexWriterConfig getConfig() {
        ensureOpen();
        return accumulatingIndexWriter.getConfig();
    }

    @Override
    public synchronized boolean hasPendingMerges() {
        return accumulatingIndexWriter.hasPendingMerges();
    }

    // Since we are doing a commit only on parent IndexWriter, in case there is any child level writers or parent writer
    // has uncommited changes, we report it as writer having uncommited changes. Since during add indexes new set of changes will be added.
    @Override
    public boolean hasUncommittedChanges() {
        // TODO: Should we do this for old writer as well?
        return hasNewIndexingOrUpdates() || accumulatingIndexWriter.hasUncommittedChanges();
    }

    @Override
    public Throwable getTragicException() {
        for (DisposableIndexWriter disposableIndexWriter : liveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values()) {
            if (disposableIndexWriter.getIndexWriter().getTragicException() != null) {
                return disposableIndexWriter.getIndexWriter().getTragicException();
            }
        }

        for (DisposableIndexWriter disposableIndexWriter : liveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap.values()) {
            if (disposableIndexWriter.getIndexWriter().getTragicException() != null) {
                return disposableIndexWriter.getIndexWriter().getTragicException();
            }
        }

        return accumulatingIndexWriter.getTragicException();
    }

    /**
     * RamBytesUsed is summation of deleteBytes, flushBytes and activeBytes.
     *
     * deleteBytes are increased when any updates are added to document. flushBytes are increased post indexing in case
     * a DWPT on which indexing is happening is already marked for flush or in case we mark a dwpt for flush. ActiveBytes
     * are increased whenever indexing completes and dwpt is not marked for flush.
     *
     * @return
     */
    @Override
    public final long ramBytesUsed() {
        ensureOpen();
        return ramBytesUsedUtil(liveIndexWriterDeletesMap);
    }

    /**
     * Utility class to calculate ramBytesUsedUtil. Since we are point in time current instance of
     * LiveIndexWriterDeletesMap, we should not worry about double counting due to map rotation from current to old. Also
     * when old will be synced with parent writer, old writer is closed so we do not need to worry about double counting
     * in parent and old IndexWriters.
     *
     * @param currentLiveIndexWriterDeletesMap current point in time instance of LiveIndexWriterDeletesMap.
     * @return ramBytesUsed
     */
    private long ramBytesUsedUtil(LiveIndexWriterDeletesMap currentLiveIndexWriterDeletesMap) {
        long ramBytesUsed = 0;
        for (DisposableIndexWriter disposableIndexWriter : currentLiveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values()) {
            try {
                ramBytesUsed += disposableIndexWriter.getIndexWriter().ramBytesUsed();
            } catch (AlreadyClosedException e) {
                if (disposableIndexWriter.getIndexWriter().getTragicException() != null) {
                    throw e;
                }
            }
        }

        for (DisposableIndexWriter disposableIndexWriter : currentLiveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap.values()) {
            try {
                ramBytesUsed += disposableIndexWriter.getIndexWriter().ramBytesUsed();
            } catch (AlreadyClosedException e) {
                if (disposableIndexWriter.getIndexWriter().getTragicException() != null) {
                    throw e;
                }
            }
        }

        return ramBytesUsed + accumulatingIndexWriter.ramBytesUsed();
    }

    // We always set live commit data for parent writer as we are commiting data only in parent writer (after refreshing child level
    // writers).
    @Override
    public final synchronized void setLiveCommitData(Iterable<Map.Entry<String, String>> commitUserData) {
        accumulatingIndexWriter.setLiveCommitData(commitUserData);
    }

    @Override
    public final long commit() throws IOException {
        ensureOpen();
        return accumulatingIndexWriter.commit();
    }

    @Override
    public final synchronized Iterable<Map.Entry<String, String>> getLiveCommitData() {
        return accumulatingIndexWriter.getLiveCommitData();
    }

    public void rollback() throws IOException {
        if (shouldClose()) {
            // Though calling rollback on child level writer seems like a redundant thing, but this ensures all the
            // child level IndexWriters are closed and there is no case of file leaks. Incase rollback throws any
            // exception close the shard as rollback gets called in close flow. And rollback throwin exception means
            // there is already leaks.
            for (DisposableIndexWriter disposableIndexWriter : liveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values()) {
                disposableIndexWriter.getIndexWriter().rollback();
            }

            for (DisposableIndexWriter disposableIndexWriter : liveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap.values()) {
                disposableIndexWriter.getIndexWriter().rollback();
            }

            accumulatingIndexWriter.rollback();
            closed.set(true);
        }
    }

    private boolean shouldClose() {
        return closed.get() == false;
    }

    private void ensureOpen() throws AlreadyClosedException {
        if (closed.get() == true) {
            throw new AlreadyClosedException("CompositeIndexWriter is closed");
        }
    }

    public boolean isOpen() {
        return closed.get() == false;
    }

    public boolean isWriteLockedByCurrentThread() {
        return liveIndexWriterDeletesMap.current.mapLock.isWriteLockedByCurrentThread()
            || liveIndexWriterDeletesMap.old.mapLock.isWriteLockedByCurrentThread();
    }

    @Override
    public Releasable obtainWriteLockOnAllMap() {
        ReleasableLock lock1 = this.getOldWriteLock().acquire();
        ReleasableLock lock2 = this.getNewWriteLock().acquire();
        return () -> {
            lock1.close();
            lock2.close();
        };
    }

    @Override
    public void close() throws IOException {
        rollback();
        liveIndexWriterDeletesMap = new LiveIndexWriterDeletesMap();
    }

    @Override
    public synchronized void deleteUnusedFiles() throws IOException {
        accumulatingIndexWriter.deleteUnusedFiles();
    }

    public IndexWriter getAccumulatingIndexWriter() {
        return accumulatingIndexWriter;
    }

    @Override
    public long addDocuments(final List<ParseContext.Document> docs, Term uid) throws IOException {
        // We obtain a read lock on a child level IndexWriter and then return it. Post Indexing completes, we close this
        // IndexWriter.
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(docs.iterator().next());
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            long seqNo = disposableIndexWriter.getIndexWriter().addDocuments(docs);
            childWriterPendingNumDocs.addAndGet(docs.size());
            return seqNo;
        }
    }

    @Override
    public long addDocument(ParseContext.Document doc, Term uid) throws IOException {
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(doc);
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            long seqNo = disposableIndexWriter.getIndexWriter().addDocument(doc);
            childWriterPendingNumDocs.incrementAndGet();
            return seqNo;
        }
    }

    @Override
    public void softUpdateDocuments(
        Term uid,
        List<ParseContext.Document> docs,
        long version,
        long seqNo,
        long primaryTerm,
        Field... softDeletesField
    ) throws IOException {
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(docs.iterator().next());
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            disposableIndexWriter.getIndexWriter().softUpdateDocuments(uid, docs, softDeletesField);
            childWriterPendingNumDocs.addAndGet(docs.size());
            // TODO: Do we need to add more info in delete entry like id, seqNo, primaryTerm for debugging??
            // TODO: Entry can be null for first version or if there is term bum up (validate if this is because we need to keep previous
            // version).
            // Validate if this is going wrong?? Last entry should be checked to handle scenario when there is a indexing post delete.
            disposableIndexWriter.getLookupMap().putLastDeleteEntry(uid.bytes(), new DeleteEntry(uid, version, seqNo, primaryTerm));
        }
    }

    @Override
    public void softUpdateDocument(
        Term uid,
        ParseContext.Document doc,
        long version,
        long seqNo,
        long primaryTerm,
        Field... softDeletesField
    ) throws IOException {
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(doc);
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            disposableIndexWriter.getIndexWriter().softUpdateDocument(uid, doc, softDeletesField);
            childWriterPendingNumDocs.incrementAndGet();
            // TODO: Do we need to add more info in delete entry like id, seqNo, primaryTerm for debugging??
            // TODO: Entry can be null for first version or if there is term bum up (validate if this is because we need to keep previous
            // version).
            // Validate if this is going wrong?? Last entry should be checked to handle scenario when there is a indexing post delete.
            disposableIndexWriter.getLookupMap().putLastDeleteEntry(uid.bytes(), new DeleteEntry(uid, version, seqNo, primaryTerm));
        }
    }

    /**
     * For deleteDocument call, we will take a lock on current writer, do a partial delete of the
     * document (delete without indexing tombstone entry). We do a similar thing for old map IndexWriter. For parent, we
     * do a full delete (delete doc + tombstone entry). This ensures only a single tombstone entry is made after delete
     * operation. Also doing a full delete on parent ensures, that accumulating IndexWriter is never left in an
     * inconsistent state (which may become an issue with segrep).
     *
     * @param uid uid of the document that is getting deleted.
     * @param isStaleOperation signify if this is a stale operation (say if document is already deleted).
     * @param doc tombstone entry.
     * @param softDeletesField the soft delete field
     *
     * @throws IOException if there is a low-level IO error.
     */
    @Override
    public void deleteDocument(
        Term uid,
        boolean isStaleOperation,
        ParseContext.Document doc,
        long version,
        long seqNo,
        long primaryTerm,
        Field... softDeletesField
    ) throws IOException {
        ensureOpen();
        CompositeIndexWriter.DisposableIndexWriter currentDisposableWriter = getIndexWriterForIdFromCurrent(uid.bytes());
        if (currentDisposableWriter != null) {
            try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignore = currentDisposableWriter.getLookupMap().getMapReadLock()) {
                if (currentDisposableWriter.getLookupMap().isClosed() == false && isStaleOperation == false) {
                    addDeleteEntryToWriter(new DeleteEntry(uid, version, seqNo, primaryTerm), currentDisposableWriter.getIndexWriter());
                    // only increment this when addDeleteEntry for child writers are called.
                    childWriterPendingNumDocs.incrementAndGet();
                }
            }
        }

        CompositeIndexWriter.DisposableIndexWriter oldDisposableWriter = getIndexWriterForIdFromOld(uid.bytes());
        if (oldDisposableWriter != null) {
            try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignore = oldDisposableWriter.getLookupMap().getMapReadLock()) {
                if (oldDisposableWriter.getLookupMap().isClosed() == false && isStaleOperation == false) {
                    addDeleteEntryToWriter(new DeleteEntry(uid, version, seqNo, primaryTerm), oldDisposableWriter.getIndexWriter());
                    // only increment this when addDeleteEntry for child writers are called.
                    childWriterPendingNumDocs.incrementAndGet();
                }
            }
        }

        deleteInLucene(uid, isStaleOperation, accumulatingIndexWriter, doc, softDeletesField);
    }

    private void deleteInLucene(
        Term uid,
        boolean isStaleOperation,
        IndexWriter currentWriter,
        Iterable<? extends IndexableField> doc,
        Field... softDeletesField
    ) throws IOException {
        if (isStaleOperation) {
            currentWriter.addDocument(doc);
        } else {
            currentWriter.softUpdateDocument(uid, doc, softDeletesField);
        }

        childWriterPendingNumDocs.incrementAndGet();
    }

    private DisposableIndexWriter getAssociatedIndexWriterForCriteria(final String criteria) throws IOException {
        return computeIndexWriterIfAbsentForCriteria(criteria, childIndexWriterFactory);
    }

    private String getGroupingCriteriaForDoc(final ParseContext.Document doc) {
        return doc == null ? null : doc.getGroupingCriteria();
    }

    @Override
    public void forceMergeDeletes(boolean doWait) throws IOException {
        accumulatingIndexWriter.forceMergeDeletes(doWait);
    }

    @Override
    public final void maybeMerge() throws IOException {
        ensureOpen();
        accumulatingIndexWriter.maybeMerge();
    }

    @Override
    public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
        ensureOpen();
        accumulatingIndexWriter.forceMerge(maxNumSegments, doWait);
    }

    CompositeIndexWriter.DisposableIndexWriter createChildWriterUtil(
        String associatedCriteria,
        CompositeIndexWriter.CriteriaBasedIndexWriterLookup lookup
    ) throws IOException {
        return new CompositeIndexWriter.DisposableIndexWriter(
            nativeIndexWriterFactory.createWriter(
                store.newTempDirectory(CHILD_DIRECTORY_PREFIX + associatedCriteria + "_" + UUID.randomUUID()),
                new OpenSearchConcurrentMergeScheduler(
                    engineConfig.getShardId(),
                    engineConfig.getIndexSettings(),
                    engineConfig.getMergedSegmentTransferTracker()
                ),
                true,
                IndexWriterConfig.OpenMode.CREATE,
                null,
                softDeletesPolicy,
                config(),
                logger,
                associatedCriteria
            ),
            lookup
        );
    }

    private EngineConfig config() {
        return engineConfig;
    }
}
