/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.bytes;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.hamcrest.Matchers;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

public class ByteBuffersBytesReferenceTests extends AbstractBytesReferenceTestCase {
    @ParametersFactory
    public static Collection<Object[]> allocator() {
        return Arrays.asList(
            new Object[] { (Function<Integer, ByteBuffer>) ByteBuffer::allocateDirect },
            new Object[] { (Function<Integer, ByteBuffer>) ByteBuffer::allocate }
        );
    }

    private final Function<Integer, ByteBuffer> allocator;

    public ByteBuffersBytesReferenceTests(Function<Integer, ByteBuffer> allocator) {
        this.allocator = allocator;
    }

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReference(length, randomInt(length));
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        return newBytesReference(length, 0);
    }

    private BytesReference newBytesReference(int length, int offset) throws IOException {
        // we know bytes stream output always creates a paged bytes reference, we use it to create randomized content
        final ByteBuffer buffer = allocator.apply(length + offset);
        for (int i = 0; i < length + offset; i++) {
            buffer.put((byte) random().nextInt(1 << 8));
        }
        assertEquals(length + offset, buffer.limit());
        buffer.flip().position(offset);

        BytesReference ref = BytesReference.fromByteBuffer(buffer);
        assertEquals(length, ref.length());
        assertTrue(ref instanceof BytesArray);
        assertThat(ref.length(), Matchers.equalTo(length));
        return ref;
    }

    public void testArray() throws IOException {
        int[] sizes = { 0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5)) };

        for (int i = 0; i < sizes.length; i++) {
            BytesArray pbr = (BytesArray) newBytesReference(sizes[i]);
            byte[] array = pbr.array();
            assertNotNull(array);
            assertEquals(sizes[i], array.length - pbr.offset());
            assertSame(array, pbr.array());
        }
    }

    public void testArrayOffset() throws IOException {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesArray pbr = (BytesArray) newBytesReferenceWithOffsetOfZero(length);
        assertEquals(0, pbr.offset());
    }
}
