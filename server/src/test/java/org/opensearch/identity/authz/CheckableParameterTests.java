/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity.authz;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CheckableParameterTests extends OpenSearchTestCase {
    public void testWriteCheckableParameterToStream() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        CheckableParameter<Integer> paramOut = new CheckableParameter<>("param1", 1, Integer.class);
        CheckableParameter.writeParameterToStream(paramOut, out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        CheckableParameter<Integer> paramIn = null;
        try {
            paramIn = CheckableParameter.readParameterFromStream(in);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        assertEquals("param1", paramIn.getKey());
        assertEquals(Integer.class, paramIn.getType());
        assertEquals(Integer.valueOf(1), paramIn.getValue());
    }
}
