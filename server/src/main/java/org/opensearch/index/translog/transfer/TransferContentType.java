/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

/**
 * Enum to denote the type of data to be transferred.
 * Used in cases where depending on data, transfer differs such as metadata is never encrypted
 * whereas data is encrypted and transferred.
 */
public enum TransferContentType {
    METADATA,
    DATA
}
