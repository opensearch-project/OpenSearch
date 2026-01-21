/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.stream;

import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;

public class MetadataWriteableTests extends OpenSearchTestCase {

    /**
     * Simple test implementation of MetadataWriteable.
     */
    private static class TestMetadataWriteable implements MetadataWriteable {
        private final String value;

        TestMetadataWriteable(String value) {
            this.value = value;
        }

        TestMetadataWriteable(StreamInput in) throws IOException {
            this.value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }

        @Override
        public void writeToMetadataStream(StreamOutput out) throws IOException {
            writeTo(out);
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestMetadataWriteable that = (TestMetadataWriteable) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        static final MetadataReader<TestMetadataWriteable> METADATA_READER = TestMetadataWriteable::new;
    }

    public void testMetadataWriteableRoundTrip() throws IOException {
        final TestMetadataWriteable before = new TestMetadataWriteable(randomAlphaOfLengthBetween(5, 20));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeToMetadataStream(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final TestMetadataWriteable after = TestMetadataWriteable.METADATA_READER.readFromMetadataStream(in);

        assertThat(after, equalTo(before));
        assertThat(after.getValue(), equalTo(before.getValue()));
    }

    public void testMetadataReaderFunctionalInterface() throws IOException {
        // Test that MetadataReader works as a functional interface
        MetadataWriteable.MetadataReader<TestMetadataWriteable> reader = in -> new TestMetadataWriteable(in.readString());

        final String testValue = randomAlphaOfLengthBetween(5, 20);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        out.writeString(testValue);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final TestMetadataWriteable result = reader.readFromMetadataStream(in);

        assertThat(result.getValue(), equalTo(testValue));
    }

    public void testWriteToMetadataStreamDelegatesToWriteTo() throws IOException {
        final TestMetadataWriteable item = new TestMetadataWriteable(randomAlphaOfLengthBetween(5, 20));

        // Write using writeTo
        final ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        final StreamOutput out1 = new OutputStreamStreamOutput(baos1);
        item.writeTo(out1);
        out1.close();

        // Write using writeToMetadataStream
        final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        final StreamOutput out2 = new OutputStreamStreamOutput(baos2);
        item.writeToMetadataStream(out2);
        out2.close();

        // Both should produce identical bytes
        assertArrayEquals(baos1.toByteArray(), baos2.toByteArray());
    }
}
