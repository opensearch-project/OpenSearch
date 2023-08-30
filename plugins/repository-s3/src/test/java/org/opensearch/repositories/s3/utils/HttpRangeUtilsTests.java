/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

import software.amazon.awssdk.core.exception.SdkException;

import org.opensearch.common.collect.Tuple;
import org.opensearch.test.OpenSearchTestCase;

public final class HttpRangeUtilsTests extends OpenSearchTestCase {

    public void testFromHttpRangeHeader() {
        String headerValue = "bytes 0-10/200";
        Tuple<Long, Long> range = HttpRangeUtils.fromHttpRangeHeader(headerValue);
        assertEquals(0L, range.v1().longValue());
        assertEquals(10L, range.v2().longValue());

        headerValue = "bytes 0-10/*";
        range = HttpRangeUtils.fromHttpRangeHeader(headerValue);
        assertEquals(0L, range.v1().longValue());
        assertEquals(10L, range.v2().longValue());

        final String invalidHeaderValue = "bytes */*";
        assertThrows(SdkException.class, () -> HttpRangeUtils.fromHttpRangeHeader(invalidHeaderValue));
    }
}
