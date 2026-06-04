/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.concurrent;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.FileSystem;

import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class GatedCloseableTests extends OpenSearchTestCase {

    private FileSystem testRef;
    GatedCloseable<FileSystem> testObject;

    @Before
    public void setup() {
        testRef = mock(FileSystem.class);
        testObject = new GatedCloseable<>(testRef, testRef::close);
    }

    public void testGet() throws Exception {
        assertNotNull(testObject.get());
        assertEquals(testRef, testObject.get());
        verify(testRef, never()).close();
    }

    public void testClose() throws IOException {
        testObject.close();
        verify(testRef, atMostOnce()).close();
    }

    public void testIdempotent() throws IOException {
        testObject.close();
        testObject.close();
        verify(testRef, atMostOnce()).close();
    }

    public void testException() throws IOException {
        doThrow(new IOException()).when(testRef).close();
        assertThrows(IOException.class, () -> testObject.close());
    }
}
