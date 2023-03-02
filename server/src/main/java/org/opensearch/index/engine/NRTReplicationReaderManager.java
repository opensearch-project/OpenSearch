/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.StandardDirectoryReader;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This is an extension of {@link OpenSearchReaderManager} for use with {@link NRTReplicationEngine}.
 * The manager holds a reference to the latest {@link SegmentInfos} object that is used to refresh a reader.
 *
 * @opensearch.internal
 */
public class NRTReplicationReaderManager extends OpenSearchReaderManager {

    private final static Logger logger = LogManager.getLogger(NRTReplicationReaderManager.class);
    private volatile SegmentInfos currentInfos;

    /**
     * Creates and returns a new SegmentReplicationReaderManager from the given
     * already-opened {@link OpenSearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader the SegmentReplicationReaderManager to use for future reopens
     */
    NRTReplicationReaderManager(OpenSearchDirectoryReader reader) {
        super(reader);
        currentInfos = unwrapStandardReader(reader).getSegmentInfos();
    }

    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        Objects.requireNonNull(referenceToRefresh);
        // checks if an actual refresh (change in segments) happened
        if (unwrapStandardReader(referenceToRefresh).getSegmentInfos().version == currentInfos.version) {
            return null;
        }
        final List<LeafReader> subs = new ArrayList<>();
        final StandardDirectoryReader standardDirectoryReader = unwrapStandardReader(referenceToRefresh);
        for (LeafReaderContext ctx : standardDirectoryReader.leaves()) {
            subs.add(ctx.reader());
        }
        DirectoryReader innerReader = StandardDirectoryReader.open(referenceToRefresh.directory(), currentInfos, subs, null);
        final DirectoryReader softDeletesDirectoryReaderWrapper = new SoftDeletesDirectoryReaderWrapper(
            innerReader,
            Lucene.SOFT_DELETES_FIELD
        );
        logger.trace(
            () -> new ParameterizedMessage("updated to SegmentInfosVersion=" + currentInfos.getVersion() + " reader=" + innerReader)
        );
        return OpenSearchDirectoryReader.wrap(softDeletesDirectoryReaderWrapper, referenceToRefresh.shardId());
    }

    /**
     * Update this reader's segments and refresh.
     *
     * @param infos {@link SegmentInfos} infos
     * @throws IOException - When Refresh fails with an IOException.
     */
    public synchronized void updateSegments(SegmentInfos infos) throws IOException {
        // roll over the currentInfo's generation, this ensures the on-disk gen
        // is always increased.
        infos.updateGeneration(currentInfos);
        currentInfos = infos;
        maybeRefresh();
    }

    public SegmentInfos getSegmentInfos() {
        return currentInfos;
    }

    private StandardDirectoryReader unwrapStandardReader(OpenSearchDirectoryReader reader) {
        final DirectoryReader delegate = reader.getDelegate();
        if (delegate instanceof SoftDeletesDirectoryReaderWrapper) {
            return (StandardDirectoryReader) ((SoftDeletesDirectoryReaderWrapper) delegate).getDelegate();
        }
        return (StandardDirectoryReader) delegate;
    }
}
