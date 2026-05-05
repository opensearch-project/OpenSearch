/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.apache.arrow.memory.BufferAllocator;

/** Wrapper exposing a {@link BufferAllocator} to Guice for injection into example actions. */
final class ExampleAllocator {
    private final BufferAllocator allocator;

    ExampleAllocator(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    BufferAllocator get() {
        return allocator;
    }
}
