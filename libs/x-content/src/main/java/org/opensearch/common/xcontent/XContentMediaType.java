/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

/**
 * Abstracts a <a href="http://en.wikipedia.org/wiki/Internet_media_type">Media Type</a> support
 * for {@link XContent}
 */
public interface XContentMediaType extends MediaType {
    /**
     * Return the {@link XContent} that corresponds this media type
     * @return {@link XContent} that corresponds this media type
     */
    XContent xContent();
}
