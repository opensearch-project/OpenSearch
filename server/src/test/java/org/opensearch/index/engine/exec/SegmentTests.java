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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link Segment} serialization.
 */
public class SegmentTests extends OpenSearchTestCase {

    private static final String TEST_DIRECTORY = "/tmp/test-segment";

    public void testCopyWriteable() throws Exception {
        Segment original = randomSegment();
        Segment copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new Segment(in, key -> TEST_DIRECTORY)
        );
        assertEquals(original, copy);
    }

    public void testCopyWriteableEmpty() throws Exception {
        Segment empty = new Segment(0L, Map.of());
        Segment copy = copyWriteable(
            empty,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new Segment(in, key -> TEST_DIRECTORY)
        );
        assertEquals(empty, copy);
    }

    public void testCopyWriteableMultiFormat() throws Exception {
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        dfGrouped.put("lucene", randomWriterFileSet("lucene"));
        dfGrouped.put("parquet", randomWriterFileSet("parquet"));
        Segment original = new Segment(randomNonNegativeLong(), dfGrouped);

        Segment copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new Segment(in, key -> TEST_DIRECTORY)
        );
        assertEquals(original, copy);
        assertEquals(2, copy.dfGroupedSearchableFiles().size());
    }

    // --- helpers ---

    private WriterFileSet randomWriterFileSet(String format) {
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        String[] extensions = "lucene".equals(format) ? new String[] { "cfs", "si", "dat" } : new String[] { "parquet" };
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom(extensions));
        }
        return new WriterFileSet(TEST_DIRECTORY, randomNonNegativeLong(), files, randomIntBetween(0, 10000), 0L);
    }

    private Segment randomSegment() {
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            String format = randomFrom("lucene", "parquet");
            dfGrouped.put(format, randomWriterFileSet(format));
        }
        return new Segment(randomNonNegativeLong(), dfGrouped);
    }
}
