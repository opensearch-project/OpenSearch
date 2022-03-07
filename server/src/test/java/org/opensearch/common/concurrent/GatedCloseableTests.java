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

package org.opensearch.common.concurrent;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

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
