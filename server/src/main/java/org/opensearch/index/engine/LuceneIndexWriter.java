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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.Map;

public class LuceneIndexWriter implements DocumentIndexWriter {
    private final IndexWriter indexWriter;

    public LuceneIndexWriter(IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }


    @Override
    public long getFlushingBytes() {
        return indexWriter.getFlushingBytes();
    }

    @Override
    public long getPendingNumDocs() {
        return indexWriter.getPendingNumDocs();
    }

    @Override
    public LiveIndexWriterConfig getConfig() {
        return indexWriter.getConfig();
    }

    @Override
    public boolean hasPendingMerges() {
        return indexWriter.hasPendingMerges();
    }

    @Override
    public boolean hasUncommittedChanges() {
        return indexWriter.hasUncommittedChanges();
    }

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
    public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Term uid) throws IOException {
        return indexWriter.addDocuments(docs);
    }

    @Override
    public long addDocument(Iterable<? extends IndexableField> doc, Term uid) throws IOException {
        return indexWriter.addDocument(doc);
    }

    @Override
    public void softUpdateDocuments(Term uid, Iterable<? extends Iterable<? extends IndexableField>> docs, long version, long seqNo, long primaryTerm, Field... softDeletesField) throws IOException {
        indexWriter.softUpdateDocuments(uid, docs);
    }

    @Override
    public void softUpdateDocument(Term uid, Iterable<? extends IndexableField> doc, long version, long seqNo, long primaryTerm, Field... softDeletesField) throws IOException {
        indexWriter.softUpdateDocument(uid, doc);
    }

    @Override
    public void deleteDocument(Term uid, boolean isStaleOperation, Iterable<? extends IndexableField> doc, long version, long seqNo, long primaryTerm, Field... softDeletesField) throws IOException {
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
}
