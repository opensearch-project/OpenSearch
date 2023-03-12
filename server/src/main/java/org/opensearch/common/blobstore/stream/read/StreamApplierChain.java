/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

import org.opensearch.common.Stream;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * ABCDE
 *
 * @opensearch.internal
 */
public class StreamApplierChain {

    private final List<StreamApplier> streamAppliers;

    /**
     * ABCDE
     *
     * @param streamAppliers
     */
    public StreamApplierChain(StreamApplier... streamAppliers) {
        this.streamAppliers = Arrays.asList(streamAppliers);
    }

    /**
     * ABCDE
     *
     * @param streamApplier
     */
    public void addStreamApplier(StreamApplier streamApplier) {
        streamAppliers.add(streamApplier);
    }

    /**
     * ABCDE
     *
     * @param stream
     * @return
     */
    public InputStream applyAll(Stream stream) {
        if (streamAppliers == null) {
            return stream.getInputStream();
        }

        for (StreamApplier streamApplier : streamAppliers) {
            stream = streamApplier.apply(stream);
        }

        return stream.getInputStream();
    }
}
