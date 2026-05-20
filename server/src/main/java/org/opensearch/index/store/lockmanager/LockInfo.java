/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.common.annotation.PublicApi;

/**
 * An Interface that defines Remote Store Lock Information.
 * Individual Implemented Classes of this interface can decide how the lock should look like and its contents.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public interface LockInfo {
    /**
     * A function which generates the lock name on the basis of given information.
     * @return the name of the lock.
     */
    String generateLockName();

    /**
     * An Interface that defines a Lock Info Builder.
     */
    public static interface LockInfoBuilder {
        /**
         * Method to build Lock Info Instance.
         */
        public LockInfo build();
    }
}
