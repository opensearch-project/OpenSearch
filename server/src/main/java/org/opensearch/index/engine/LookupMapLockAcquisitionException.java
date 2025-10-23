/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

/**
 * This exception indicates that CompositeIndexWriter was unable to obtain lock on CriteriaBasedIndexWriterLookup map
 * during indexing.
 * indexing request contains this Exception in the response, we do not need to add a translog entry for this request.
 *
 */
public class LookupMapLockAcquisitionException extends IllegalStateException {
    public LookupMapLockAcquisitionException(String message) {
        super(message);
    }
}
