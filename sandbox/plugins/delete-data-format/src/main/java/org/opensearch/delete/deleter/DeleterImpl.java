/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.delete.deleter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.Writer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Default {@link Deleter} implementation that routes delete operations based on the
 * underlying {@link Writer}'s capabilities, using only interface methods to avoid
 * cross-plugin classloader issues.
 * <p>
 * At construction, resolves a Lucene-capable writer via {@link Writer#findWriterByFormat(String)}.
 * If found, deletes delegate to {@link Writer#deleteDocument(Term)}; otherwise,
 * deletes fall back to row-ID tracking for formats like Parquet.
 *
 * @opensearch.experimental
 */
public class DeleterImpl implements Deleter {
    private static final Logger logger = LogManager.getLogger(DeleterImpl.class);
    private final Set<Long> deletedRowIds = new HashSet<>();
    private final Writer<?> luceneWriter;
    private final long deleterGeneration;

    /**
     * Creates a deleter paired with the given writer. Resolves a Lucene-capable writer
     * at construction time via {@link Writer#findWriterByFormat(String)}, avoiding
     * {@code instanceof} checks against plugin classes.
     *
     * @param writer the writer this deleter is associated with
     */
    public DeleterImpl(Writer<?> writer) {
        this.luceneWriter = writer.findWriterByFormat("lucene").orElse(null);
        this.deleterGeneration = writer.generation();
    }

    @Override
    public long generation() {
        return this.deleterGeneration;
    }

    /**
     * Returns the set of row IDs that have been deleted by this deleter.
     *
     * @return the deleted row IDs
     */
    public Set<Long> getDeletedRowIds() {
        return deletedRowIds;
    }

    @Override
    public DeleteResult deleteDoc(Term uid, Long rowId) throws IOException {
        if (luceneWriter != null) {
            luceneWriter.deleteDocument(uid);
        } else {
            deletedRowIds.add(rowId);
        }
        return new DeleteResult.Success(1L, 1L, 1L);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void lock() {}

    @Override
    public boolean tryLock() {
        return false;
    }

    @Override
    public void unlock() {}
}
