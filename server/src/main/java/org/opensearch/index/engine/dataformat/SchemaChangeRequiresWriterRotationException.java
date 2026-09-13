/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Thrown by {@link Writer#updateMappingVersion(long)} when a mapping-driven schema change
 * cannot be safely patched into the writer's already-open generation.
 *
 * <p>Some formats match part of their on-disk schema by position rather than by name (e.g.
 * Parquet's nested {@code LIST<STRUCT>} children, matched positionally by Substrait/DataFusion
 * on read). Patching a newly-mapped field into an active writer is only safe if it lands in the
 * position a freshly-built, fully sorted schema would give it; otherwise the writer's on-disk
 * order would desync from every other generation's and corrupt reads.
 *
 * <p>The thrower leaves its generation's schema untouched. The caller (see
 * {@code DataFormatAwareEngine#indexIntoEngine}) must flush and retire that writer, obtain a
 * fresh one (built from the current mapping, already correctly sorted), and retry the
 * triggering document there exactly once.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SchemaChangeRequiresWriterRotationException extends RuntimeException {

    /**
     * Creates a new SchemaChangeRequiresWriterRotationException.
     *
     * @param message description of which field/position triggered the rotation requirement
     */
    public SchemaChangeRequiresWriterRotationException(String message) {
        super(message);
    }
}
