/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.util.set.Sets;
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

public class AliasMetadataModelTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final AliasMetadataModel before = new AliasMetadataModel.Builder("alias").filter(createTestFilter())
            .indexRouting("indexRouting")
            .routing("routing")
            .searchRouting("trim,tw , ltw , lw")
            .writeIndex(randomBoolean() ? null : randomBoolean())
            .isHidden(randomBoolean() ? null : randomBoolean())
            .build();

        assertThat(before.searchRoutingValues(), equalTo(Sets.newHashSet("trim", "tw ", " ltw ", " lw")));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final AliasMetadataModel after = new AliasMetadataModel(in);

        assertThat(after, equalTo(before));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testEquals() {
        AliasMetadataModel model1 = createTestItem();
        AliasMetadataModel model2 = new AliasMetadataModel(
            model1.alias(),
            model1.filter(),
            model1.indexRouting(),
            model1.searchRouting(),
            model1.writeIndex(),
            model1.isHidden()
        );

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    private static AliasMetadataModel createTestItem() {
        AliasMetadataModel.Builder builder = new AliasMetadataModel.Builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.filter(createTestFilter());
        }
        builder.writeIndex(randomBoolean());

        if (randomBoolean()) {
            builder.isHidden(randomBoolean());
        }
        return builder.build();
    }

    private static CompressedData createTestFilter() {
        return new CompressedData(randomByteArrayOfLength(randomIntBetween(10, 50)), randomInt());
    }
}
