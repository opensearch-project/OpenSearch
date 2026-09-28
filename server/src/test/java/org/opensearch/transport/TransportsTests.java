/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

public class TransportsTests extends OpenSearchTestCase {

    public void testAssertDefaultThreadContextAllowsTaskRequestHeaders() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(Task.X_OPAQUE_ID, "opaque-id");
        threadContext.putHeader(Task.X_REQUEST_ID, "1234567890abcdef1234567890abcdef");

        assertTrue(Transports.assertDefaultThreadContext(threadContext));
    }

    public void testAssertDefaultThreadContextRejectsNonTaskRequestHeaders() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("custom-header", "value");

        expectThrows(AssertionError.class, () -> Transports.assertDefaultThreadContext(threadContext));
    }
}
