/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;

/** test the BytesReferenceStream using the same BaseStreamTests */
public class BytesReferenceStreamInputTests extends BaseStreamTests {
    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        return bytesReference.streamInput();
    }
}
