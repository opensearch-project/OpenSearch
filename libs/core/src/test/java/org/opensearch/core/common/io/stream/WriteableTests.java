package org.opensearch.core.common.io.stream;

import org.junit.Assert;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

public class WriteableTests extends OpenSearchTestCase {

    public void testRegisterClassAlias() {
        Writeable.WriteableRegistry.registerClassAlias(StringBuilder.class, AtomicInteger.class);
        try {
            Writeable.WriteableRegistry.registerClassAlias(StringBuilder.class, AtomicInteger.class);
            Assert.fail("expected exception not thrown");
        } catch (IllegalArgumentException illegalArgumentException) {
            Assert.assertEquals("Streamable custom class already registered [java.lang.StringBuilder]", illegalArgumentException.getMessage());
        }
    }
}
