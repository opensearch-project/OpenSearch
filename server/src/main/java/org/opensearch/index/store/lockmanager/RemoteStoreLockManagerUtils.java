/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

/**
 * Utility class for remote store lock manager,
 * right now only have constants defined, we can add methods as well here in the future.
 * @opensearch.internal
 */
public class RemoteStoreLockManagerUtils {
    static final String SEPARATOR = "___";
    static final String LOCK_FILE_EXTENSION = ".lock";
}
