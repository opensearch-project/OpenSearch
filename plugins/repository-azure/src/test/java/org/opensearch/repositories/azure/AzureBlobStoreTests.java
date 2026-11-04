/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import org.opensearch.test.OpenSearchTestCase;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class AzureBlobStoreTests extends OpenSearchTestCase {

    public void testInputStreamMethodDescriptor() throws ReflectiveOperationException {
        final Method method = AzureBlobStore.class.getMethod("getInputStream", String.class, long.class, Long.class);
        assertEquals(InputStream.class, method.getReturnType());
        assertTrue(Modifier.isPublic(method.getModifiers()));
    }
}
