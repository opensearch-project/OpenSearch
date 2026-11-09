/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.util.Version;
import org.opensearch.test.OpenSearchTestCase;

public class LuceneVersionConverterTests extends OpenSearchTestCase {

    public void testNullReturnsLatest() {
        assertEquals(Version.LATEST, LuceneVersionConverter.toLuceneOrLatest(null));
    }

    public void testEmptyReturnsLatest() {
        assertEquals(Version.LATEST, LuceneVersionConverter.toLuceneOrLatest(""));
    }

    public void testValidLuceneStringParses() throws Exception {
        Version expected = Version.parse("10.4.0");
        assertEquals(expected, LuceneVersionConverter.toLuceneOrLatest("10.4.0"));
    }

    public void testGarbageStringReturnsLatest() {
        assertEquals(Version.LATEST, LuceneVersionConverter.toLuceneOrLatest("not-a-version"));
    }

    public void testParquetStyleDoesNotThrow() {
        // "1.0.0.0" is not a valid Lucene version — should fall back to LATEST without throwing
        Version result = LuceneVersionConverter.toLuceneOrLatest("1.0.0.0");
        assertNotNull(result);
    }
}
