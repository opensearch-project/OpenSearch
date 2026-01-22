/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.metadata.compress.CompressedData;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class MappingMetadataModelTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final MappingMetadataModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final MappingMetadataModel after = new MappingMetadataModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.type(), equalTo(before.type()));
        assertThat(after.source(), equalTo(before.source()));
        assertThat(after.routingRequired(), equalTo(before.routingRequired()));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testEquals() {
        MappingMetadataModel model1 = createTestItem();
        MappingMetadataModel model2 = new MappingMetadataModel(model1.type(), model1.source(), model1.routingRequired());

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    private static MappingMetadataModel createTestItem() {
        return new MappingMetadataModel(randomAlphaOfLengthBetween(3, 10), createTestSource(), randomBoolean());
    }

    private static CompressedData createTestSource() {
        return new CompressedData(randomByteArrayOfLength(randomIntBetween(10, 100)), randomInt());
    }
}
