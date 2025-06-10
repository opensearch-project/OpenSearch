/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

/**
 * An enum for the ingest pipeline type
 */
public enum IngestPipelineType {
    /**
     * Default pipeline is the pipeline provided through the index request or defined in
     * the index settings as the default pipeline.
     */
    DEFAULT,
    /**
     * Final pipeline is the one defined in the index settings as the final pipeline.
     */
    FINAL,
    /**
     * System final pipeline is a systematically generated pipeline which will be executed after the
     * user defined final pipeline.
     */
    SYSTEM_FINAL
}
