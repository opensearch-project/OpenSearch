/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link WriterFileSet}.
 */
public class WriterFileSetTests extends OpenSearchTestCase {

    public void testCopyWriteable() throws Exception {
        WriterFileSet original = randomWriterFileSet();
        WriterFileSet copy = copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()), WriterFileSet::new);
        assertEquals(original, copy);
    }

    // --- helpers ---

    private WriterFileSet randomWriterFileSet() {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom("cfs", "si", "dat", "parquet"));
        }
        return new WriterFileSet(directory, randomNonNegativeLong(), files, randomIntBetween(0, 10000));
    }
}
