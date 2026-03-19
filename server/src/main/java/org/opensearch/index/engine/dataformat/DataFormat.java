/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Set;

/**
 * Describes a pluggable data format (e.g. parquet, arrow, lucene).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormat {

    String name();

    long priority();

    Set<FieldTypeCapabilities> supportedFields();
}
