/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.write;

import org.opensearch.common.StreamContext;

/**
 * Will return the <code>StreamContext</code> to the caller given the part size
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface StreamContextSupplier {

    /**
     * @param partSize The size of a single part to be uploaded
     * @return The <code>StreamContext</code> based on the part size provided
     */
    StreamContext supplyStreamContext(long partSize);
}
