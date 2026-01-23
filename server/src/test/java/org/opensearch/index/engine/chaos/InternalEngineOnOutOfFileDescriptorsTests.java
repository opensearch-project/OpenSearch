/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.chaos;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterUtil;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.common.util.FeatureFlags.CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG;

public class InternalEngineOnOutOfFileDescriptorsTests extends EngineTestCase {

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testAddDocumentOnOutOfFileDescriptors() throws IOException {
        double rate = 0.01;
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new ByteBuffersDirectory());
        dir.setRandomIOExceptionRateOnOpen(rate);
        IndexWriterFactory indexWriterFactory = (directory, iwc) -> {
            MergeScheduler ms = iwc.getMergeScheduler();
            IndexWriterUtil.suppressMergePolicyException(ms);
            return new IndexWriter(directory, iwc);
        };
        boolean hitException = false;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultSettings.getSettings())
                .put(IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        try (
            Store store = createStore(dir);
            InternalEngine engine = createEngine(
                indexSettings,
                store,
                primaryTranslogDir,
                newMergePolicy(),
                indexWriterFactory,
                null,
                globalCheckpoint::get
            )
        ) {
            int numDocsFirstSegment = randomIntBetween(50, 100);
            try {
                for (int i = 0; i < numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), B_1, null);
                    engine.index(indexForDoc(doc));
                }
            } catch (IOException ex) {
                hitException = true;
            }

            assertFalse(hitException);
            assertTrue(DirectoryReader.indexExists(dir));

            try {
                engine.refresh("testing");
            } catch (EngineException e) {
                hitException = true;
            }

            assertTrue(DirectoryReader.indexExists(dir));
            rate = 1.0;
            dir.setRandomIOExceptionRateOnOpen(rate);
            try {
                for (int i = numDocsFirstSegment; i < numDocsFirstSegment + numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), B_1, null);
                    engine.index(indexForDoc(doc));
                }

                engine.refresh("testing");
            } catch (EngineException e) {
                hitException = true;
            }

            assertTrue(hitException);
            assertTrue(DirectoryReader.indexExists(dir));
        }
    }
}
