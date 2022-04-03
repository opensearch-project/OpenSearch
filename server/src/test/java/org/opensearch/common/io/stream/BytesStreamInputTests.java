/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.bytes.BytesReference;

import java.io.IOException;

/** test the BytesStreamInput using the same BaseStreamTests */
public class BytesStreamInputTests extends BaseStreamTests {
    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        BytesRef br = bytesReference.toBytesRef();
        return new BytesStreamInput(br.bytes, br.offset, br.length);
    }
}
