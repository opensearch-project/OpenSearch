/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.fielddata.cache;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.index.Index;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

public class IndicesFieldDataCacheAccountingAuditTests extends OpenSearchTestCase {

    public void testAuditWarnsOnlyOnSustainedDrift() throws Exception {
        final IndicesFieldDataCache fdCache = new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
        }, null, null);
        // A noop breaker always estimates 0 used bytes, so any cached entry registers as drift.
        final CircuitBreaker underestimatingBreaker = new NoopCircuitBreaker(CircuitBreaker.FIELDDATA);
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
            writer.addDocument(new Document());
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                IndicesFieldDataCache.IndexFieldCache indexCache = (IndicesFieldDataCache.IndexFieldCache) fdCache.buildIndexFieldDataCache(
                    new IndexFieldDataCache.Listener() {
                    },
                    new Index("index", "_na_"),
                    "field"
                );
                IndicesFieldDataCache.Key key = new IndicesFieldDataCache.Key(indexCache, reader.getReaderCacheHelper().getKey(), null);
                fdCache.getCache().put(key, () -> 1024L);

                // A single drifting audit can be a transient race with an in-flight load or removal: no warning.
                auditExpectingWarning(fdCache, underestimatingBreaker, false);
                // Drift persisting across consecutive audits is a leak: warn.
                auditExpectingWarning(fdCache, underestimatingBreaker, true);

                // Accounting agreeing again resets the streak.
                fdCache.getCache().invalidateAll();
                auditExpectingWarning(fdCache, underestimatingBreaker, false);

                // New drift must again persist for DRIFT_AUDITS_BEFORE_WARN audits before warning.
                fdCache.getCache().put(key, () -> 2048L);
                auditExpectingWarning(fdCache, underestimatingBreaker, false);
                auditExpectingWarning(fdCache, underestimatingBreaker, true);
            }
        } finally {
            fdCache.close();
        }
    }

    private void auditExpectingWarning(IndicesFieldDataCache fdCache, CircuitBreaker breaker, boolean expectWarning) throws Exception {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(IndicesFieldDataCache.class))) {
            final String loggerName = IndicesFieldDataCache.class.getCanonicalName();
            final String pattern = "*accounting has drifted*";
            appender.addExpectation(
                expectWarning
                    ? new MockLogAppender.SeenEventExpectation("drift warning", loggerName, Level.WARN, pattern)
                    : new MockLogAppender.UnseenEventExpectation("no drift warning", loggerName, Level.WARN, pattern)
            );
            fdCache.auditAccounting(breaker);
            appender.assertAllExpectationsMatched();
        }
    }
}
