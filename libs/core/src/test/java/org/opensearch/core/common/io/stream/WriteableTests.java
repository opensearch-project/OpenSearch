/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicInteger;

public class WriteableTests extends OpenSearchTestCase {

    public void testRegisterClassAlias() {
        Writeable.WriteableRegistry.registerClassAlias(StringBuilder.class, AtomicInteger.class);
        try {
            Writeable.WriteableRegistry.registerClassAlias(StringBuilder.class, AtomicInteger.class);
            Assert.fail("expected exception not thrown");
        } catch (IllegalArgumentException illegalArgumentException) {
            Assert.assertEquals(
                "Streamable custom class already registered [java.lang.StringBuilder]",
                illegalArgumentException.getMessage()
            );
        }
    }
}
