/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Utility class for checking file types
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FileTypeUtils {

    public static boolean isTempFile(String name) {
        return name.endsWith(".tmp");
    }

    public static boolean isBlockFile(String name) {
        return name.contains("_block_");
    }

    public static boolean isExtraFSFile(String name) {
        return name.startsWith("extra");
    }

    public static boolean isLockFile(String name) {
        return name.endsWith(".lock");
    }

    public static boolean isSegmentsFile(String name) {
        return name.startsWith("segments_");
    }
}
