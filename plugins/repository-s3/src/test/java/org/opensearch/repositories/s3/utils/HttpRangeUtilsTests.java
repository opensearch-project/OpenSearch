/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

import software.amazon.awssdk.core.exception.SdkException;

import org.opensearch.test.OpenSearchTestCase;

public final class HttpRangeUtilsTests extends OpenSearchTestCase {

    public void testFromHttpRangeHeader() {
        String headerValue = "bytes 0-10/200";
        Long offset = HttpRangeUtils.getStartOffsetFromRangeHeader(headerValue);
        assertEquals(0L, offset.longValue());

        headerValue = "bytes 0-10/*";
        offset = HttpRangeUtils.getStartOffsetFromRangeHeader(headerValue);
        assertEquals(0L, offset.longValue());

        final String invalidHeaderValue = "bytes */*";
        assertThrows(SdkException.class, () -> HttpRangeUtils.getStartOffsetFromRangeHeader(invalidHeaderValue));
    }
}
