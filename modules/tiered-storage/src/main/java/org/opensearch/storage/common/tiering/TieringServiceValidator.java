/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Validator for tiering service operations in OpenSearch.
 */
public class TieringServiceValidator {

    private static final Logger logger = LogManager.getLogger(TieringServiceValidator.class);
    private static final long GB_IN_BYTES = 1024 * 1024 * 1024;
    private static final long MIN_DISK_SPACE_BUFFER = 20 * GB_IN_BYTES;
    private static final double CLUSTER_WRITE_BLOCK_THRESHOLD_MULTIPLIER = 0.2;
    private static final long DEFAULT_FALLBACK_SHARD_SIZE = 0L;

    /** Private constructor. */
    private TieringServiceValidator() {}
}
