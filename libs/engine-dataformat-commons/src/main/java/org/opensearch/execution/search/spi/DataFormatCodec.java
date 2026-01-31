/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.execution.search.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.execution.search.DataFormat;

import java.util.concurrent.CompletableFuture;

/**
 * Service Provider Interface for data source codecs.
 * Contains configurations , optimizers etc to support query of different data formats
 * through the pluggable engines.
 */
@ExperimentalApi
public interface DataFormatCodec {
    /**
     * Returns the data format name
     */
    DataFormat getDataFormat();

    CompletableFuture<Void> closeSessionContext(long sessionContextId);
}
