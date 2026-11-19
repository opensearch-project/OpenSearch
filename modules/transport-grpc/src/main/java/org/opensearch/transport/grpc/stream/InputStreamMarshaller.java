/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import java.io.InputStream;

import io.grpc.MethodDescriptor;

/**
 * Passes message streams through unchanged, so a method can produce its wire bytes directly.
 */
public final class InputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {

    /** The shared instance. */
    public static final InputStreamMarshaller INSTANCE = new InputStreamMarshaller();

    private InputStreamMarshaller() {}

    @Override
    public InputStream stream(InputStream value) {
        return value;
    }

    @Override
    public InputStream parse(InputStream stream) {
        return stream;
    }
}
