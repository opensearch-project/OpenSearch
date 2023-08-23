/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.util.Version;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.index.store.Store;

import java.io.IOException;

public class NRTReplicationReaderManagerTests extends EngineTestCase {

    public void testCreateNRTreaderManager() throws IOException {
        try (final Store store = createStore()) {
            store.createEmpty(Version.LATEST);
            final DirectoryReader reader = DirectoryReader.open(store.directory());
            final SegmentInfos initialInfos = ((StandardDirectoryReader) reader).getSegmentInfos();
            NRTReplicationReaderManager readerManager = new NRTReplicationReaderManager(
                OpenSearchDirectoryReader.wrap(reader, shardId),
                (files) -> {},
                (files) -> {}
            );
            assertEquals(initialInfos, readerManager.getSegmentInfos());
            try (final OpenSearchDirectoryReader acquire = readerManager.acquire()) {
                assertNull(readerManager.refreshIfNeeded(acquire));
            }

            // create an updated infos
            final SegmentInfos infos_2 = readerManager.getSegmentInfos().clone();
            infos_2.changed();

            readerManager.updateSegments(infos_2);
            assertEquals(infos_2, readerManager.getSegmentInfos());
            try (final OpenSearchDirectoryReader acquire = readerManager.acquire()) {
                final StandardDirectoryReader standardReader = NRTReplicationReaderManager.unwrapStandardReader(acquire);
                assertEquals(infos_2, standardReader.getSegmentInfos());
            }
        }
    }
}
