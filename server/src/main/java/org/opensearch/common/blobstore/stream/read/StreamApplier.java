/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

import org.opensearch.common.Stream;

/**
 * ABCDE
 *
 * @opensearch.internal
 */
public interface StreamApplier {

    /**
     * ABCDE
     *
     * @param stream
     * @return
     */
    Stream apply(Stream stream);
}
