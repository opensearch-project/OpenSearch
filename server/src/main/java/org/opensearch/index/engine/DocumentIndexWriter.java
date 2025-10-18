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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Primary interface for document indexing operation in OpenSearch. This interface is mapped after Lucene's IndexWriter.
 *
 */
public interface DocumentIndexWriter extends Closeable {

    long getFlushingBytes();

    long getPendingNumDocs();

    LiveIndexWriterConfig getConfig();

    boolean hasPendingMerges();

    boolean hasUncommittedChanges();

    Throwable getTragicException();

    long ramBytesUsed();

    void setLiveCommitData(Iterable<Map.Entry<String, String>> commitUserData);

    long commit() throws IOException;

    Iterable<Map.Entry<String, String>> getLiveCommitData();

    void rollback() throws IOException;

    void close() throws IOException;

    void deleteUnusedFiles() throws IOException;

    long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Term uid) throws IOException;

    long addDocument(Iterable<? extends IndexableField> doc, Term uid) throws IOException;

    void softUpdateDocuments(
        Term uid,
        Iterable<? extends Iterable<? extends IndexableField>> docs,
        long version,
        long seqNo,
        long primaryTerm,
        Field... softDeletesField
    ) throws IOException;

    void softUpdateDocument(
        Term uid,
        Iterable<? extends IndexableField> doc,
        long version,
        long seqNo,
        long primaryTerm,
        Field... softDeletesField
    ) throws IOException;

    void deleteDocument(
        Term uid,
        boolean isStaleOperation,
        Iterable<? extends IndexableField> doc,
        long version,
        long seqNo,
        long primaryTerm,
        Field... softDeletesField
    ) throws IOException;

    void forceMergeDeletes(boolean doWait) throws IOException;

    void maybeMerge() throws IOException;

    void forceMerge(int maxNumSegments, boolean doWait) throws IOException;

    IndexWriter getAccumulatingIndexWriter();

}
