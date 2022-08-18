/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Translog Factory to enable creation of various local on-disk
 * and remote store flavors of {@link Translog}
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface TranslogFactory {

    Translog newTranslog(
        final TranslogConfig config,
        final String translogUUID,
        final TranslogDeletionPolicy deletionPolicy,
        final LongSupplier globalCheckpointSupplier,
        final LongSupplier primaryTermSupplier,
        final LongConsumer persistedSequenceNumberConsumer
    ) throws IOException;
}
