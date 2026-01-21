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
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ContextModelTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final ContextModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.name(), equalTo(before.name()));
        assertThat(after.version(), equalTo(before.version()));
        assertThat(after.params(), equalTo(before.params()));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testEquals() {
        ContextModel model1 = createTestItem();
        ContextModel model2 = new ContextModel(model1.name(), model1.version(), model1.params());

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    public void testSerializationWithNullVersion() throws IOException {
        final ContextModel before = new ContextModel("test-context", null, Collections.emptyMap());

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.name(), equalTo("test-context"));
        assertNull(after.version());
        assertTrue(after.params().isEmpty());
    }

    public void testSerializationWithParams() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("stringParam", "value");
        params.put("intParam", 42);
        params.put("boolParam", true);

        final ContextModel before = new ContextModel("test-context", "1.0", params);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertEquals("value", after.params().get("stringParam"));
        assertEquals(42, after.params().get("intParam"));
        assertEquals(true, after.params().get("boolParam"));
    }

    public void testSerializationWithMixedParamTypes() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("string", "hello");
        params.put("int", 123);
        params.put("long", 9876543210L);
        params.put("double", 3.14159);
        params.put("boolean", false);
        params.put("list", Arrays.asList("a", "b", "c"));

        final ContextModel before = new ContextModel("mixed-context", "2.0", params);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertEquals("hello", after.params().get("string"));
        assertEquals(123, after.params().get("int"));
        assertEquals(9876543210L, after.params().get("long"));
        assertEquals(3.14159, after.params().get("double"));
        assertEquals(false, after.params().get("boolean"));
        assertEquals(Arrays.asList("a", "b", "c"), after.params().get("list"));
    }

    public void testNotEquals() {
        ContextModel model1 = new ContextModel("name1", "1.0", Collections.emptyMap());
        ContextModel model2 = new ContextModel("name2", "1.0", Collections.emptyMap());
        ContextModel model3 = new ContextModel("name1", "2.0", Collections.emptyMap());

        assertNotEquals(model1, model2);
        assertNotEquals(model1, model3);
        assertNotEquals(model2, model3);
    }

    private static ContextModel createTestItem() {
        String name = randomAlphaOfLengthBetween(3, 10);
        String version = randomBoolean() ? randomAlphaOfLengthBetween(1, 5) : null;
        Map<String, Object> params = new HashMap<>();
        if (randomBoolean()) {
            int numParams = randomIntBetween(1, 5);
            for (int i = 0; i < numParams; i++) {
                params.put(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 20));
            }
        }
        return new ContextModel(name, version, params);
    }
}
