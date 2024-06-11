/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.compress;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.spi.CompressorProvider;
import org.opensearch.core.xcontent.MediaTypeRegistry;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * A registry that wraps a static Map singleton which holds a mapping of unique String names (typically the
 * compressor header as a string) to registerd {@link Compressor} implementations.
 * <p>
 * This enables plugins, modules, extensions to register their own compression implementations through SPI
 *
 * @opensearch.experimental
 * @opensearch.internal
 */
@InternalApi
public final class CompressorRegistry {

    // the backing registry map
    private static final Map<String, Compressor> registeredCompressors = ServiceLoader.load(
        CompressorProvider.class,
        CompressorProvider.class.getClassLoader()
    )
        .stream()
        .flatMap(p -> p.get().getCompressors().stream())
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    // no instance:
    private CompressorRegistry() {}

    /**
     * Returns the default compressor
     */
    public static Compressor defaultCompressor() {
        return registeredCompressors.get("DEFLATE");
    }

    public static Compressor none() {
        return registeredCompressors.get(NoneCompressor.NAME);
    }

    public static boolean isCompressed(BytesReference bytes) {
        return compressor(bytes) != null;
    }

    @Nullable
    public static Compressor compressor(final BytesReference bytes) {
        for (Compressor compressor : registeredCompressors.values()) {
            if (compressor.isCompressed(bytes) == true) {
                // bytes should be either detected as compressed or as xcontent,
                // if we have bytes that can be either detected as compressed or
                // as a xcontent, we have a problem
                assert MediaTypeRegistry.xContentType(bytes) == null;
                return compressor;
            }
        }

        if (MediaTypeRegistry.xContentType(bytes) == null) {
            throw new NotXContentException("Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes");
        }

        return null;
    }

    /**
     * @param bytes The bytes to check the compression for
     * @return The detected compressor. If no compressor detected then return NoneCompressor.
     */
    public static Compressor compressorForWritable(final BytesReference bytes) {
        for (Compressor compressor : registeredCompressors.values()) {
            if (compressor.isCompressed(bytes) == true) {
                return compressor;
            }
        }
        return CompressorRegistry.none();
    }

    /** Decompress the provided {@link BytesReference}. */
    public static BytesReference uncompress(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(bytes);
        if (compressor == null) {
            throw new NotCompressedException();
        }
        return compressor.uncompress(bytes);
    }

    /**
     * Uncompress the provided data, data can be detected as compressed using {@link #isCompressed(BytesReference)}.
     */
    public static BytesReference uncompressIfNeeded(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(Objects.requireNonNull(bytes, "the BytesReference must not be null"));
        return compressor == null ? bytes : compressor.uncompress(bytes);
    }

    /** Returns a registered compressor by its registered name */
    public static Compressor getCompressor(final String name) {
        if (registeredCompressors.containsKey(name)) {
            return registeredCompressors.get(name);
        }
        throw new IllegalArgumentException("No registered compressor found by name [" + name + "]");
    }

    /**
     * Returns the registered compressors as an Immutable collection
     * <p>
     * note: used for testing
     */
    public static Map<String, Compressor> registeredCompressors() {
        // no destructive danger as backing map is immutable
        return registeredCompressors;
    }
}
