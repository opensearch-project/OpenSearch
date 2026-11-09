/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

public class WriteableBase64Tests extends OpenSearchTestCase {

    /** A minimal {@link Writeable} used to exercise the codec independently of any production type. */
    private static class Sample implements Writeable {
        private final String name;
        private final long value;

        Sample(String name, long value) {
            this.name = name;
            this.value = value;
        }

        Sample(StreamInput in) throws IOException {
            this.name = in.readString();
            this.value = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeLong(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Sample sample = (Sample) o;
            return value == sample.value && Objects.equals(name, sample.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }
    }

    public void testRoundTrip() throws IOException {
        Sample original = new Sample(randomAlphaOfLength(12), randomLong());

        String encoded = WriteableBase64.encode(original);
        Sample decoded = WriteableBase64.decode(encoded, Sample::new);

        assertEquals(original, decoded);
    }

    public void testEncodeProducesValidBase64() throws IOException {
        Sample original = new Sample("action", 42L);

        String encoded = WriteableBase64.encode(original);

        // Must decode as standard Base64 without throwing; the resulting bytes reconstruct the object.
        byte[] decodedBytes = Base64.getDecoder().decode(encoded);
        try (StreamInput in = StreamInput.wrap(decodedBytes)) {
            assertEquals(original, new Sample(in));
        }
    }

    public void testEmptyStringThrows() {
        // An empty payload has no bytes for the reader to consume.
        expectThrows(Exception.class, () -> WriteableBase64.decode("", Sample::new));
    }

    public void testInvalidBase64Throws() {
        expectThrows(IllegalArgumentException.class, () -> WriteableBase64.decode("not-valid-base64!!!", Sample::new));
    }
}
