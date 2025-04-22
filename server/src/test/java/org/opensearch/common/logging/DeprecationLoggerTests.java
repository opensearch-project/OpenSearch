/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DeprecationLoggerTests extends OpenSearchTestCase {

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);

        DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DeprecationLoggerTests.class);
        int numberOfLoggersBefore = context.getLoggers().size();

        class LoggerTest {}
        DeprecationLogger deprecationLogger2 = DeprecationLogger.getLogger(LoggerTest.class);

        context = (LoggerContext) LogManager.getContext(false);
        int numberOfLoggersAfter = context.getLoggers().size();

        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore + 1));
    }

    public void testDuplicateLogMessages() {
        DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DeprecationLoggerTests.class);
        deprecationLogger.deprecate("deprecated-message-1", "Deprecated message 1");
        deprecationLogger.deprecate("deprecated-message-2", "Deprecated message 2");
        deprecationLogger.deprecate("deprecated-message-3", "Deprecated message 3");
        deprecationLogger.deprecate("deprecated-message-2", "Deprecated message 2");
        deprecationLogger.deprecate("deprecated-message-1", "Deprecated message 1");
        deprecationLogger.deprecate("deprecated-message-3", "Deprecated message 3");
        deprecationLogger.deprecate("deprecated-message-1", "Deprecated message 1");
        deprecationLogger.deprecate("deprecated-message-3", "Deprecated message 3");
        deprecationLogger.deprecate("deprecated-message-2", "Deprecated message 2");
        // assert that only unique warnings are logged
        assertWarnings("Deprecated message 1", "Deprecated message 2", "Deprecated message 3");
    }

    public void testMaximumSizeOfCache() {
        final int maxEntries = DeprecatedMessage.MAX_DEDUPE_CACHE_ENTRIES;
        // Fill up the cache, asserting every message is new
        for (int i = 0; i < maxEntries; i++) {
            DeprecatedMessage message = new DeprecatedMessage("key-" + i, "message-" + i, "");
            assertFalse(message.toString(), message.isAlreadyLogged());
        }
        // Do the same thing except assert every message has been seen
        for (int i = 0; i < maxEntries; i++) {
            DeprecatedMessage message = new DeprecatedMessage("key-" + i, "message-" + i, "");
            assertTrue(message.toString(), message.isAlreadyLogged());
        }
        // Add one more new entry, asserting it will forever been seen as already logged (cache is full)
        DeprecatedMessage message = new DeprecatedMessage("key-new", "message-new", "");
        assertTrue(message.toString(), message.isAlreadyLogged());
        assertTrue(message.toString(), message.isAlreadyLogged());
    }
}
