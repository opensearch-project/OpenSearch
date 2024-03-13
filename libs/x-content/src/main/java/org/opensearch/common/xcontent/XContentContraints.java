/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import com.fasterxml.jackson.core.StreamReadConstraints;

import org.opensearch.common.annotation.InternalApi;

/**
 * Consolidates the XContent constraints (primarily reflecting Jackson's {@link StreamReadConstraints} constraints)
 *
 * @opensearch.internal
 */
@InternalApi
public interface XContentContraints {
    final String DEFAULT_CODEPOINT_LIMIT_PROPERTY = "opensearch.xcontent.codepoint.max";
    final String DEFAULT_MAX_STRING_LEN_PROPERTY = "opensearch.xcontent.string.length.max";
    final String DEFAULT_MAX_NAME_LEN_PROPERTY = "opensearch.xcontent.name.length.max";
    final String DEFAULT_MAX_DEPTH_PROPERTY = "opensearch.xcontent.depth.max";

    final int DEFAULT_MAX_STRING_LEN = Integer.parseInt(System.getProperty(DEFAULT_MAX_STRING_LEN_PROPERTY, "50000000" /* ~50 Mb */));

    final int DEFAULT_MAX_NAME_LEN = Integer.parseInt(
        System.getProperty(DEFAULT_MAX_NAME_LEN_PROPERTY, "50000" /* StreamReadConstraints.DEFAULT_MAX_NAME_LEN */)
    );

    final int DEFAULT_MAX_DEPTH = Integer.parseInt(
        System.getProperty(DEFAULT_MAX_DEPTH_PROPERTY, "1000" /* StreamReadConstraints.DEFAULT_MAX_DEPTH */)
    );

    final int DEFAULT_CODEPOINT_LIMIT = Integer.parseInt(System.getProperty(DEFAULT_CODEPOINT_LIMIT_PROPERTY, "52428800" /* ~50 Mb */));
}
