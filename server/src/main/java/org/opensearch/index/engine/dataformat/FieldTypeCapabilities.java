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
 * Describes what a data format can do with a particular field type.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record FieldTypeCapabilities(String fieldType, Set<Capability> capabilities) {

    public enum Capability {
        COLUMNAR_STORAGE,
        STORED_FIELDS,
        DOC_VALUES,
        INVERTED_INDEX
    }
}
