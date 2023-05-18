/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.io.IOException;

/**
 * TransferPartStreamSupplier is used to supply streams to specific parts of a file based on
 * the partNo, size and position (the offset in the file)
 */
public interface TransferPartStreamSupplier {
    OffsetStreamContainer supply(int partNo, long size, long position) throws IOException;
}
