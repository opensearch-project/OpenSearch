/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Translog Factory to enable creation of various local on-disk
 * and remote store flavors of {@link Translog}
 *
 * @opensearch.api
 */
@FunctionalInterface
@PublicApi(since = "1.0.0")
public interface TranslogFactory {

    Translog newTranslog(
        final TranslogConfig config,
        final String translogUUID,
        final TranslogDeletionPolicy deletionPolicy,
        final LongSupplier globalCheckpointSupplier,
        final LongSupplier primaryTermSupplier,
        final LongConsumer persistedSequenceNumberConsumer,
        final BooleanSupplier primaryModeSupplier
    ) throws IOException;
}
