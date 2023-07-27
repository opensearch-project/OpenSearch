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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * This is an extension of {@link OpenSearchReaderManager} for use with {@link NRTReplicationEngine}.
 * The manager holds a reference to the latest {@link SegmentInfos} object that is used to refresh a reader.
 *
 * @opensearch.internal
 */
public class NRTReplicationReaderManager extends OpenSearchReaderManager {

    private final static Logger logger = LogManager.getLogger(NRTReplicationReaderManager.class);
    private volatile SegmentInfos currentInfos;
    private Consumer<Collection<String>> onReaderClosed;
    private Consumer<Collection<String>> onNewReader;

    /**
     * Creates and returns a new SegmentReplicationReaderManager from the given
     * already-opened {@link OpenSearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader         - The SegmentReplicationReaderManager to use for future reopens.
     * @param onNewReader    - Called when a new reader is created.
     * @param onReaderClosed - Called when a reader is closed.
     */
    NRTReplicationReaderManager(
        OpenSearchDirectoryReader reader,
        Consumer<Collection<String>> onNewReader,
        Consumer<Collection<String>> onReaderClosed
    ) {
        super(reader);
        currentInfos = unwrapStandardReader(reader).getSegmentInfos();
        this.onNewReader = onNewReader;
        this.onReaderClosed = onReaderClosed;
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
        // Segment_n here is ignored because it is either already committed on disk as part of previous commit point or
        // does not yet exist on store (not yet committed)
        final Collection<String> files = currentInfos.files(false);
        DirectoryReader innerReader = StandardDirectoryReader.open(referenceToRefresh.directory(), currentInfos, subs, null);
        final DirectoryReader softDeletesDirectoryReaderWrapper = new SoftDeletesDirectoryReaderWrapper(
            innerReader,
            Lucene.SOFT_DELETES_FIELD
        );
        logger.trace(
            () -> new ParameterizedMessage("updated to SegmentInfosVersion=" + currentInfos.getVersion() + " reader=" + innerReader)
        );
        final OpenSearchDirectoryReader reader = OpenSearchDirectoryReader.wrap(
            softDeletesDirectoryReaderWrapper,
            referenceToRefresh.shardId()
        );
        onNewReader.accept(files);
        OpenSearchDirectoryReader.addReaderCloseListener(reader, key -> onReaderClosed.accept(files));
        return reader;
    }

    /**
     * Update this reader's segments and refresh.
     *
     * @param infos {@link SegmentInfos} infos
     * @throws IOException - When Refresh fails with an IOException.
     */
    public void updateSegments(SegmentInfos infos) throws IOException {
        // roll over the currentInfo's generation, this ensures the on-disk gen
        // is always increased.
        infos.updateGeneration(currentInfos);
        currentInfos = infos;
        maybeRefresh();
    }

    public SegmentInfos getSegmentInfos() {
        return currentInfos;
    }

    public static StandardDirectoryReader unwrapStandardReader(OpenSearchDirectoryReader reader) {
        final DirectoryReader delegate = reader.getDelegate();
        if (delegate instanceof SoftDeletesDirectoryReaderWrapper) {
            return (StandardDirectoryReader) ((SoftDeletesDirectoryReaderWrapper) delegate).getDelegate();
        }
        return (StandardDirectoryReader) delegate;
    }
}
