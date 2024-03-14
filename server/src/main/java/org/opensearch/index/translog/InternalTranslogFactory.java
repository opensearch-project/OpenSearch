/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Translog Factory for the local on-disk {@link Translog}
 *
 * @opensearch.internal
 */
public class InternalTranslogFactory implements TranslogFactory {

    @Override
    public Translog newTranslog(
        TranslogConfig translogConfig,
        String translogUUID,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier
    ) throws IOException {

        return new LocalTranslog(
            translogConfig,
            translogUUID,
            translogDeletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer
        );
    }
}
