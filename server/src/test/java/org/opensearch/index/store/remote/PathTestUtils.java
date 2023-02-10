/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.Assert.fail;

public final class PathTestUtils {
    /**
     * Cleans directory recursively
     * @param path to delete
     */
    public static void cleanDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.delete(f);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /**
     * Cleans directory recursively or fail test when exception is thrown
     * @param path to delete
     */
    public static void cleanOrFail(Path path) {
        try {
            cleanDirectory(path);
        } catch (Exception e) {
            fail();
        }
    }

    /**
     * create Lucene temp dir to be used in testing
     * @param pathName to be created
     * @return the path created
     */
    public static Path createTestPath(String pathName) {
        return LuceneTestCase.createTempDir(pathName);
    }
}
