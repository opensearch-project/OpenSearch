/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.chaos;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.common.util.FeatureFlags.CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG;

public class InternalEngineOnJRECrashTests extends EngineTestCase {

    private void runIndexingWorkload(Engine engine, AtomicInteger operationCount) throws IOException {
        long i = 0;
        while (!Thread.currentThread().isInterrupted()) {
            final String id = "docid#" + i;
            ParsedDocument doc = testParsedDocument(
                id,
                null,
                testContextSpecificDocumentWithTenantField("grouping_criteria"),
                TENANT_SOURCE,
                null
            );
            engine.index(indexForDoc(doc));
            operationCount.incrementAndGet();
            i++;
            if (i % 100 == 0) {
                engine.refresh("testing");
            }
        }
    }

    public void verifyDataPersistenceAfterCrash(int crashDelayMs) throws Exception {
        final Path storeDirPath = createTempDir();
        final Path translogPath = createTempDir();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        AtomicInteger operationCount = new AtomicInteger(0);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultSettings.getSettings())
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .put(IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING.getKey(), true)
                .build()
        );

        try (Store store = createStore(newFSDirectory(storeDirPath))) {
            try (
                InternalEngine engine = createEngine(
                    config(indexSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpoint::get)
                )
            ) {
                CountDownLatch started = new CountDownLatch(1);
                CountDownLatch finished = new CountDownLatch(1);
                // Thread that runs the actual test
                Thread testThread = new Thread(() -> {
                    try {
                        started.countDown();
                        runIndexingWorkload(engine, operationCount);
                    } catch (IOException ignore) {
                        logger.error("Ignoring exception " + ignore.getMessage(), ignore);
                    } finally {
                        finished.countDown();
                    }
                }, "engine-test-thread");

                testThread.setUncaughtExceptionHandler(
                    (t, e) -> { logger.info("Ignoring uncaught exception in an interrupted thread", e); }
                );

                // Start the indexing thread.
                testThread.start();
                // Wait for the indexing thread to start.
                started.await();

                // Wait main thread for specified time period, so that write continues on indexing thread.
                Thread.sleep(crashDelayMs);

                // Interrupt the thread.
                testThread.interrupt();
                // Wait for thread to finish (with timeout)
                boolean threadFinished = finished.await(5, TimeUnit.SECONDS);
                if (!threadFinished) {
                    logger.warn("Thread did not finish in time");
                }
            }
        }

        // Validate if there is no corruption on JVM crash.
        // Do we need to remove Write.lock file incase rollback did not happened correctly.
        // TODO: Validate if doc count remained same post jvm crash via translog replay (Validate JVM crash prevents any
        // translog entry to be written to disk from memory).
        try (Directory directory = newFSDirectory(storeDirPath)) {
            assertTrue(DirectoryReader.indexExists(directory));
        }
    }

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testEngineOperationsForJRECrashAfterDelay_10ms() throws Exception {
        verifyDataPersistenceAfterCrash(10);
    }

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testEngineOperationsForJRECrashAfterDelay_100ms() throws Exception {
        verifyDataPersistenceAfterCrash(100);
    }

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testEngineOperationsForJRECrashAfterDelay_1000ms() throws Exception {
        verifyDataPersistenceAfterCrash(1000);
    }

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testEngineOperationsForJRECrashAfterDelay_10000ms() throws Exception {
        verifyDataPersistenceAfterCrash(10000);
    }
}
