/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authz;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

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
