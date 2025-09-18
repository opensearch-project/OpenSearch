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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.KeyedLock;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.Assertions;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


/**
 * Maps _uid value to its deletes information. It also contains information on IndexWriter.
 *
 */
public class CompositeIndexWriter implements ReferenceManager.RefreshListener, Closeable {

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private final EngineConfig engineConfig;
    private final IndexWriter accumulatingIndexWriter;
    private final CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> childIndexWriterFactory;
    private final NumericDocValuesField softDeletesField;
    protected final Logger logger;
    private volatile boolean closed;

    public CompositeIndexWriter(EngineConfig engineConfig, IndexWriter accumulatingIndexWriter,
                                CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> childIndexWriterFactory, NumericDocValuesField softDeletesField) {
        this.engineConfig = engineConfig;
        this.accumulatingIndexWriter = accumulatingIndexWriter;
        this.childIndexWriterFactory = childIndexWriterFactory;
        this.softDeletesField = softDeletesField;
        this.logger = Loggers.getLogger(Engine.class, engineConfig.getShardId());
    }

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

    public static final class CriteriaBasedIndexWriterLookup implements Closeable {
        private final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap;
        private final Map<BytesRef, DeleteEntry> lastDeleteEntrySet;
        private final Map<BytesRef, String> criteria;
        private final ReentrantReadWriteLock mapLock;
        private final CriteriaBasedWriterLock mapReadLock;
        private final ReleasableLock mapWriteLock;
        private final long version;
        private boolean closed;

        private static final CriteriaBasedIndexWriterLookup EMPTY = new CriteriaBasedIndexWriterLookup(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), 0);

        private CriteriaBasedIndexWriterLookup(final Map<String, DisposableIndexWriter> criteriaBasedIndexWriterMap, Map<BytesRef, DeleteEntry> lastDeleteEntrySet, Map<BytesRef, String> criteria, long version) {
            this.criteriaBasedIndexWriterMap = criteriaBasedIndexWriterMap;
            this.lastDeleteEntrySet = lastDeleteEntrySet;
            this.mapLock = new ReentrantReadWriteLock();
            this.mapReadLock = new CriteriaBasedWriterLock(mapLock.readLock(), this);
            this.mapWriteLock = new ReleasableLock(mapLock.writeLock());
            this.criteria = criteria;
            this.version = version;
            this.closed = false;
        }

        DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(String criteria,
                                                                    CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) {
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
     * Map of version lookups
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
            this(new CriteriaBasedIndexWriterLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(),
                    ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(), ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(), 0),
                CriteriaBasedIndexWriterLookup.EMPTY);
        }

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        LiveIndexWriterDeletesMap buildTransitionMap() {
            // This ensures writer map is not rotated during the time when we are obtaining an IndexWriter from map. As
            // this may cause updates to go out of sync with current IndexWriter.
            return new LiveIndexWriterDeletesMap(
                    new CriteriaBasedIndexWriterLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfCriteriaBasedIndexWriterMap()),
                            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfLastDeleteEntrySet()),
                        ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.sizeOfLastDeleteEntrySet()), current.version + 1),
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

        DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(String criteria,
                                                                    CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) {
            boolean success = false;
            try {
                CriteriaBasedIndexWriterLookup current = getCurrentMap();
                assert current.isClosed() == false;
                DisposableIndexWriter writer = current.computeIndexWriterIfAbsentForCriteria(criteria, indexWriterSupplier);
                success = true;
                return writer;
            } finally {
                if (success == false && current.mapReadLock.isHeldByCurrentThread() == true) {
                    current.mapReadLock.close();
                }
            }
        }

        // This function acquires a first read lock on a map which does not have any write lock present. Current keeps
        // on getting rotated during refresh, so there will be one current on which read lock can be obtained.
        // Validate that no write lock is applied on the map and the map is not closed. Idea here is write lock was
        // never applied on this map as write lock gets only during closing time. We are doing this instead of acquire,
        // because acquire can also apply a read lock in case refresh completed and map is closed.
        CriteriaBasedIndexWriterLookup getCurrentMap() {
            return current.mapReadLock.tryAcquire();
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
        try(Releasable ignore = liveIndexWriterDeletesMap.old.mapWriteLock.acquire();
            CriteriaBasedIndexWriterLookup oldMap = liveIndexWriterDeletesMap.old;
        ) {
            logger.debug("Acquired write lock during refresh of composite IndexWriter.");
            // TODO No more write should happen post this, so that before refresh for syncing writers have all old writers available.
            // TODO Or should we do this in Reader before listner where we are syncing data??
            refreshDocumentsForParentDirectory(oldMap);
        }
    }

    private void refreshDocumentsForParentDirectory(CriteriaBasedIndexWriterLookup oldMap) throws IOException {
        final Map<String, CompositeIndexWriter.DisposableIndexWriter> markForRefreshIndexWritersMap = oldMap.criteriaBasedIndexWriterMap;
        deletePreviousVersionsForUpdatedDocuments();
        final List<Directory> directoryToCombine = new ArrayList<>();
        for (CompositeIndexWriter.DisposableIndexWriter childDisposableWriter: markForRefreshIndexWritersMap.values()) {
            directoryToCombine.add(childDisposableWriter.getIndexWriter().getDirectory());
            childDisposableWriter.getIndexWriter().close();
        }

        if (!directoryToCombine.isEmpty()) {
            accumulatingIndexWriter.addIndexes(directoryToCombine.toArray(new Directory[0]));
            IOUtils.closeWhileHandlingException(directoryToCombine);
        }
    }

    private void deletePreviousVersionsForUpdatedDocuments() throws IOException {
        Map<BytesRef, DeleteEntry> deleteEntrySet = getLastDeleteEntrySet();
        for (DeleteEntry deleteEntry: deleteEntrySet.values()) {
            // For both updates and deletes do a delete only in parent. For updates, latest writes will be on mark for flush writer,
            // do delete entry in parent. For delete, do a delete in parent. This will take care of scenario incase deleteInLucene,
            // delete went to mark for refresh.
            addDeleteEntryToWriter(deleteEntry, accumulatingIndexWriter);
        }

        Term uid = new Term(IdFieldMapper.NAME, "-2");
        accumulatingIndexWriter.deleteDocuments(uid);
    }

    /**
     * For adding delete entry, we insert a Dummy entry along with a delete.
     *
     * @param deleteEntry
     * @param currentWriter
     * @throws IOException
     */
    private void addDeleteEntryToWriter(DeleteEntry deleteEntry, IndexWriter currentWriter) throws IOException {
        Document document = new Document();
        document.add( new Field("_id", "-2", IdFieldMapper.Defaults.FIELD_TYPE));
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

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        liveIndexWriterDeletesMap = liveIndexWriterDeletesMap.invalidateOldMap();
    }

    Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    public Map<BytesRef, DeleteEntry> getLastDeleteEntrySet() {
        return liveIndexWriterDeletesMap.old.lastDeleteEntrySet;
    }

    void putLastDeleteEntryUnderLockInNewMap(BytesRef uid, DeleteEntry entry) {
        liveIndexWriterDeletesMap.putLastDeleteEntryInCurrentMap(uid, entry);
    }

    void putCriteria(BytesRef uid, String criteria) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        liveIndexWriterDeletesMap.putCriteriaForDoc(uid, criteria);
    }

    DisposableIndexWriter getIndexWriterForIdFromCurrent(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        return getIndexWriterForIdFromCurrent(uid, liveIndexWriterDeletesMap.current);
    }

    // Avoid the issue of write lock getting applied on a separate map due to map getting rotated.
    DisposableIndexWriter getIndexWriterForIdFromCurrent(BytesRef uid, CriteriaBasedIndexWriterLookup currentMaps) {
        currentMaps.mapReadLock.acquire();
        String criteria = getCriteriaForDoc(uid);
        if (criteria != null) {
            DisposableIndexWriter disposableIndexWriter = currentMaps.getIndexWriterForCriteria(criteria);
            if (disposableIndexWriter != null) {
                return disposableIndexWriter;
            }
        }

        currentMaps.mapReadLock.close();
        return null;
    }

    boolean hasNewIndexingOrUpdates() {
        return liveIndexWriterDeletesMap.hasNewIndexingOrUpdates();
    }

    String getCriteriaForDoc(BytesRef uid) {
        return liveIndexWriterDeletesMap.getCriteriaForDoc(uid);
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }

    DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(final String criteria,
                                                      CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) throws IOException {
        return computeIndexWriterIfAbsentForCriteria(criteria, liveIndexWriterDeletesMap, indexWriterSupplier);
    }

    DisposableIndexWriter computeIndexWriterIfAbsentForCriteria(final String criteria, LiveIndexWriterDeletesMap currentLiveIndexWriterDeletesMap,
                                                                CheckedBiFunction<String, CriteriaBasedIndexWriterLookup, DisposableIndexWriter, IOException> indexWriterSupplier) {
        return currentLiveIndexWriterDeletesMap.computeIndexWriterIfAbsentForCriteria(criteria, indexWriterSupplier);
    }

    public Map<String, DisposableIndexWriter> getMarkForRefreshIndexWriterMap() {
        return liveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap;
    }

    public long getFlushingBytes() {
        ensureOpen();
        long flushingBytes = 0;
        Collection<IndexWriter> currentWriterSet = liveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values()
            .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter currentWriter : currentWriterSet) {
            flushingBytes += currentWriter.getFlushingBytes();
        }

        return flushingBytes + accumulatingIndexWriter.getFlushingBytes();
    }

    public long getPendingNumDocs() {
        ensureOpen();
        long pendingNumDocs = 0;
        Collection<IndexWriter> currentWriterSet = liveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values()
            .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());;
        for (IndexWriter currentWriter : currentWriterSet) {
            pendingNumDocs += currentWriter.getPendingNumDocs();
        }

        // TODO: Should we add docs for old writer as well?
        return pendingNumDocs + accumulatingIndexWriter.getPendingNumDocs();
    }

    public LiveIndexWriterConfig getConfig() {
        ensureOpen();
        return accumulatingIndexWriter.getConfig();
    }

    public synchronized boolean hasPendingMerges() {
        return accumulatingIndexWriter.hasPendingMerges();
    }

    // Since we are doing a commit only on parent IndexWriter, in case there is any child level writers or parent writer
    // has uncommited changes, we report it as writer having uncommited changes. Since during add indexes new set of changes will be added.
    public boolean hasUncommittedChanges() {
        // TODO: Should we do this for old writer as well?
        return hasNewIndexingOrUpdates() || accumulatingIndexWriter.hasUncommittedChanges();
    }

    public Throwable getTragicException() {
        Collection<IndexWriter> currentWriterSet = liveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values().stream()
            .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        for (IndexWriter writer: currentWriterSet) {
            if (writer.isOpen() == false && writer.getTragicException() != null) {
                return writer.getTragicException();
            }
        }

        Collection<IndexWriter> oldWriterSet = liveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap.values()
            .stream().map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());;
        for (IndexWriter writer: oldWriterSet) {
            if (writer.isOpen() == false && writer.getTragicException() != null) {
                return writer.getTragicException();
            }
        }

        if (accumulatingIndexWriter.isOpen() == false) {
            return accumulatingIndexWriter.getTragicException();
        }

        return null;
    }

    public final long ramBytesUsed() {
        ensureOpen();
        long ramBytesUsed = 0;
        Collection<IndexWriter> currentWriterSet = liveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values().stream()
                .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());

        try(ReleasableLock ignore = liveIndexWriterDeletesMap.current.mapWriteLock.acquire()) {
            for (IndexWriter indexWriter : currentWriterSet) {
                if (indexWriter.isOpen() == true) {
                    ramBytesUsed += indexWriter.ramBytesUsed();
                }
            }
        }

        Collection<IndexWriter> oldWriterSet = liveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap.values().stream()
                .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
        try(ReleasableLock ignore = liveIndexWriterDeletesMap.old.mapWriteLock.acquire()) {
            for (IndexWriter indexWriter : oldWriterSet) {
                if (indexWriter.isOpen() == true) {
                    ramBytesUsed += indexWriter.ramBytesUsed();
                }
            }
        }

        return ramBytesUsed + accumulatingIndexWriter.ramBytesUsed();
    }

    // We always set live commit data for parent writer as we are commiting data only in parent writer (as refreshing child level writers).
    public final synchronized void setLiveCommitData(
        Iterable<Map.Entry<String, String>> commitUserData) {
        accumulatingIndexWriter.setLiveCommitData(commitUserData);
    }

    public final long commit() throws IOException {
        ensureOpen();
        return accumulatingIndexWriter.commit();
    }

    public final synchronized Iterable<Map.Entry<String, String>> getLiveCommitData() {
        return accumulatingIndexWriter.getLiveCommitData();
    }

    public void rollback() throws IOException {
        if (shouldClose()) {
            Collection<IndexWriter> currentWriterSet = liveIndexWriterDeletesMap.current.criteriaBasedIndexWriterMap.values().stream()
                .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());

            for (IndexWriter indexWriter : currentWriterSet) {
                if (indexWriter.isOpen() == true) {
                    indexWriter.rollback();
                }
            }

            Collection<IndexWriter> oldWriterSet = liveIndexWriterDeletesMap.old.criteriaBasedIndexWriterMap.values().stream()
                .map(DisposableIndexWriter::getIndexWriter).collect(Collectors.toSet());
            for (IndexWriter indexWriter : oldWriterSet) {
                if (indexWriter.isOpen() == true) {
                    indexWriter.rollback();
                }
            }

            accumulatingIndexWriter.rollback();
            closed = true;
        }
    }

    private boolean shouldClose() {
        return closed == false;
    }

    private void ensureOpen() throws AlreadyClosedException {
        if (closed) {
            throw new AlreadyClosedException("CompositeIndexWriter is closed");
        }
    }

    public boolean isWriteLockedByCurrentThread() {
        return liveIndexWriterDeletesMap.current.mapLock.isWriteLockedByCurrentThread() || liveIndexWriterDeletesMap.old.mapLock.isWriteLockedByCurrentThread();
    }


    @Override
    public void close() throws IOException {
        rollback();
        liveIndexWriterDeletesMap = new LiveIndexWriterDeletesMap();
    }

    public synchronized void deleteUnusedFiles() throws IOException {
        accumulatingIndexWriter.deleteUnusedFiles();
    }

    public IndexWriter getAccumulatingIndexWriter() {
        return accumulatingIndexWriter;
    }

    public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Term uid)
        throws IOException {
        // We obtain a read lock on a child level IndexWriter and then return it. Post Indexing completes, we close this
        // IndexWriter.
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(docs.iterator().next());
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            return disposableIndexWriter.getIndexWriter().addDocuments(docs);
        }
    }

    public long addDocument(Iterable<? extends IndexableField> doc, Term uid) throws IOException {
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(doc);
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            return disposableIndexWriter.getIndexWriter().addDocument(doc);
        }
    }

    public void softUpdateDocuments(
        Term uid, Iterable<? extends Iterable<? extends IndexableField>> docs, long version, long seqNo, long primaryTerm, Field... softDeletesField)
        throws IOException {
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(docs.iterator().next());
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            disposableIndexWriter.getIndexWriter().softUpdateDocuments(uid, docs, softDeletesField);
            // TODO: Do we need to add more info in delete entry like id, seqNo, primaryTerm for debugging??
            // TODO: Entry can be null for first version or if there is term bum up (validate if this is because we need to keep previous version).
            //  Validate if this is going wrong?? Last entry should be checked to handle scenario when there is a indexing post delete.
            disposableIndexWriter.getLookupMap().putLastDeleteEntry(uid.bytes(), new DeleteEntry(uid, version, seqNo, primaryTerm));
        }
    }

    public void softUpdateDocument(
        Term uid, Iterable<? extends IndexableField> doc, long version, long seqNo, long primaryTerm, Field... softDeletesField) throws IOException {
        ensureOpen();
        final String criteria = getGroupingCriteriaForDoc(doc);
        DisposableIndexWriter disposableIndexWriter = getAssociatedIndexWriterForCriteria(criteria);
        try (CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignoreLock = disposableIndexWriter.getLookupMap().getMapReadLock()) {
            putCriteria(uid.bytes(), criteria);
            disposableIndexWriter.getIndexWriter().softUpdateDocument(uid, doc, softDeletesField);
            // TODO: Do we need to add more info in delete entry like id, seqNo, primaryTerm for debugging??
            // TODO: Entry can be null for first version or if there is term bum up (validate if this is because we need to keep previous version).
            //  Validate if this is going wrong?? Last entry should be checked to handle scenario when there is a indexing post delete.
            disposableIndexWriter.getLookupMap().putLastDeleteEntry(uid.bytes(), new DeleteEntry(uid, version, seqNo, primaryTerm));
        }
    }

    public void deleteDocument(Term uid, boolean isStaleOperation, Iterable<? extends IndexableField> doc, long version, long seqNo, long primaryTerm, Field... softDeletesField) throws IOException {
        ensureOpen();
        CompositeIndexWriter.DisposableIndexWriter currentDisposableWriter = getIndexWriterForIdFromCurrent(uid.bytes());
        if (currentDisposableWriter != null) {
            try(CriteriaBasedIndexWriterLookup.CriteriaBasedWriterLock ignore = currentDisposableWriter.getLookupMap().getMapReadLock()) {
                deleteInLucene(uid, isStaleOperation, currentDisposableWriter.getIndexWriter(), doc, softDeletesField);
                // We are adding a delete entry only when we perform a soft update (delete + adding tombstone entry) on current writer.
                // For stale operation, we are not performing any delete so we skip adding delete entry.
                if (!isStaleOperation) {
                    putLastDeleteEntryUnderLockInNewMap(uid.bytes(), new DeleteEntry(uid, version, seqNo, primaryTerm));
                }
            }
        } else {
            deleteInLucene(uid, isStaleOperation, accumulatingIndexWriter, doc, softDeletesField);
            if (!isStaleOperation) {
                // Add delete entry here as well just in case there is any index writer in mark for refresh.
                putLastDeleteEntryUnderLockInNewMap(uid.bytes(), new DeleteEntry(uid, version, seqNo, primaryTerm));
            }

        }

    }

    private void deleteInLucene(Term uid, boolean isStaleOperation, IndexWriter currentWriter, Iterable<? extends IndexableField> doc, Field... softDeletesField) throws IOException {
        if (isStaleOperation) {
            currentWriter.addDocument(doc);
        } else {
            currentWriter.softUpdateDocument(uid, doc, softDeletesField);
        }
    }

    private DisposableIndexWriter getAssociatedIndexWriterForCriteria(final String criteria) throws IOException {
        return computeIndexWriterIfAbsentForCriteria(criteria, childIndexWriterFactory);
    }

    private String getGroupingCriteriaForDoc(final Iterable<? extends IndexableField> docs) {
        for (IndexableField field : docs) {
            if (field.name().equals("Marketplace")) {
                String tenantId = field.stringValue();
                if (tenantId == null || tenantId.isBlank()) {
                    return "-1";
                }

                return tenantId;
            }
        }

        return "-1";
    }

    public void forceMergeDeletes(boolean doWait) throws IOException {
        accumulatingIndexWriter.forceMergeDeletes(doWait);
    }

    public final void maybeMerge() throws IOException {
        ensureOpen();
        accumulatingIndexWriter.maybeMerge();
    }

    public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
        ensureOpen();
        accumulatingIndexWriter.forceMerge(maxNumSegments, doWait);
    }
}
