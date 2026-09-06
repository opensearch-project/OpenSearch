/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.opensearch.Version;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class AbstractWireSerializingTestCaseTests extends AbstractWireSerializingTestCase<AbstractWireSerializingTestCaseTests.Value> {
    public void testWireFixture() throws IOException {
        assertWireFixture(new Value(42), Version.CURRENT, new BytesArray(new byte[] { 0, 0, 0, 42 }));
    }

    public void testWireFixtureRejectsChangedBytes() {
        final AssertionError error = expectThrows(
            AssertionError.class,
            () -> assertWireFixture(new Value(42), Version.CURRENT, new BytesArray(new byte[] { 0, 0, 0, 41 }))
        );
        assertTrue(error.getMessage(), error.getMessage().contains("wire bytes changed"));
    }

    @Override
    protected Writeable.Reader<Value> instanceReader() {
        return Value::new;
    }

    @Override
    protected Value createTestInstance() {
        return new Value(randomInt());
    }

    static class Value implements Writeable {
        private final int value;

        Value(int value) {
            this.value = value;
        }

        Value(StreamInput input) throws IOException {
            value = input.readInt();
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeInt(value);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            Value value1 = (Value) object;
            return value == value1.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }
}
