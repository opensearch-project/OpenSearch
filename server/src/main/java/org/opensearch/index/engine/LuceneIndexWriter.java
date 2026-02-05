/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Wrapper class for IndexWriter. It delegates all calls to underlying IndexWriter.
 *
 */
public class LuceneIndexWriter implements DocumentIndexWriter {
    private final IndexWriter indexWriter;

    /**
     * Constructor for LuceneIndexWriter.
     *
     * @param indexWriter the underlying IndexWriter to which all function calls are delegated to.
     */
    public LuceneIndexWriter(IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }

    /**
     * Wrapper function over IndexWriter.getFlushingBytes.
     *
     * @return the number of bytes currently being flushed by underlying IndexWriter.
     */
    @Override
    public long getFlushingBytes() {
        return indexWriter.getFlushingBytes();
    }

    /**
     * Wrapper function over IndexWriter.getPendingNumDocs.
     *
     * @return Returns the number of documents in the index including documents are being added (i. e., reserved) for underlying IndexWriter.
     */
    @Override
    public long getPendingNumDocs() {
        return indexWriter.getPendingNumDocs();
    }

    /**
     * Wrapper function for IndexWriter.getConfig.
     *
     * @return Returns a LiveIndexWriterConfig, which can be used to query the underlying IndexWriter current settings,
     *         as well as modify "live" ones.
     */
    @Override
    public LiveIndexWriterConfig getConfig() {
        return indexWriter.getConfig();
    }

    /**
     * Wrapper function for IndexWriter.hasPendingMerges.
     *
     * @return returns true if there are merges waiting to be scheduled for underlying IndexWriter.
     */
    @Override
    public boolean hasPendingMerges() {
        return indexWriter.hasPendingMerges();
    }

    /**
     * Wrapper function for IndexWriter.hasUncommittedChanges
     *
     * @return Returns true if there may be changes that have not been committed for underlying IndexWriter.
     */
    @Override
    public boolean hasUncommittedChanges() {
        return indexWriter.hasUncommittedChanges();
    }

    /**
     * Wrapper function for IndexWriter.getTragicException
     *
     * @return Associated tragic exception for underlying IndexWriter.
     */
    @Override
    public Throwable getTragicException() {
        return indexWriter.getTragicException();
    }

    @Override
    public long ramBytesUsed() {
        return indexWriter.ramBytesUsed();
    }

    @Override
    public void setLiveCommitData(Iterable<Map.Entry<String, String>> commitUserData) {
        indexWriter.setLiveCommitData(commitUserData);
    }

    @Override
    public long commit() throws IOException {
        return indexWriter.commit();
    }

    @Override
    public Iterable<Map.Entry<String, String>> getLiveCommitData() {
        return indexWriter.getLiveCommitData();
    }

    @Override
    public void rollback() throws IOException {
        indexWriter.rollback();
    }

    @Override
    public void close() throws IOException {
        indexWriter.close();
    }

    @Override
    public void deleteUnusedFiles() throws IOException {
        indexWriter.deleteUnusedFiles();
    }

    @Override
    public long addDocuments(final List<ParseContext.Document> docs, Term uid) throws IOException {
        return indexWriter.addDocuments(docs);
    }

    @Override
    public long addDocument(ParseContext.Document doc, Term uid) throws IOException {
        return indexWriter.addDocument(doc);
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
        indexWriter.softUpdateDocuments(uid, docs, softDeletesField);
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
        indexWriter.softUpdateDocument(uid, doc, softDeletesField);
    }

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
        if (isStaleOperation) {
            indexWriter.addDocument(doc);
        } else {
            indexWriter.softUpdateDocument(uid, doc, softDeletesField);
        }
    }

    @Override
    public void forceMergeDeletes(boolean doWait) throws IOException {
        indexWriter.forceMergeDeletes(doWait);
    }

    @Override
    public void maybeMerge() throws IOException {
        indexWriter.maybeMerge();
    }

    @Override
    public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
        indexWriter.forceMerge(maxNumSegments, doWait);
    }

    @Override
    public IndexWriter getAccumulatingIndexWriter() {
        return indexWriter;
    }

    // Always return false here so that result of refreshNeeded is always equal to super.refreshNeeded()
    @Override
    public boolean hasNewIndexingOrUpdates() {
        return false;
    }

    public boolean isWriteLockedByCurrentThread() {
        return true;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Keep this no ops for Lucene IndexWriter.
    }

    @Override
    public void afterRefresh(boolean b) throws IOException {
        // Keep this no ops.
    }

    @Override
    public Releasable obtainWriteLockOnAllMap() {
        return () -> {};
    }

    public boolean validateImmutableFieldNotUpdated(ParseContext.Document previousDocument, BytesRef currentUID) {
        return false;
    }
}
