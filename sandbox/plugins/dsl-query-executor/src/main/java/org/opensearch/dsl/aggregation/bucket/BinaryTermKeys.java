/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.search.DocValueFormat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Base64;

/**
 * Shared converters for terms bucket keys that arrive as binary (ip) or string values.
 *
 * <p>The single-field {@link StringTermsStrategy} and multi-field {@link MultiTermsBucketTranslator}
 * store binary/string keys identically, so the logic lives here once: a binary key keeps its encoded
 * bytes for the mapping-resolved {@link DocValueFormat} to render at serialization, except under
 * {@link DocValueFormat#RAW} where it is pre-rendered as an address string (or a deterministic Base64
 * string when the bytes are not a valid 4- or 16-byte address) since RAW would otherwise print raw
 * bytes; a string key becomes its UTF-8 bytes. Sharing keeps a guard added for one translator from
 * silently missing the other.
 */
final class BinaryTermKeys {

    private BinaryTermKeys() {}

    /**
     * Converts a binary or string key into its bucket term bytes. Under {@link DocValueFormat#RAW} a
     * binary (ip) key is pre-rendered as its address string (Base64 fallback); otherwise the encoded
     * bytes are kept for the mapping format to render, and a non-binary key becomes its UTF-8 bytes.
     */
    static BytesRef termBytes(Object key, DocValueFormat format) {
        if (key instanceof BytesRef ref) {
            return format == DocValueFormat.RAW ? new BytesRef(binaryKeyString(ref.bytes)) : ref;
        }
        if (key instanceof byte[] bytes) {
            return format == DocValueFormat.RAW ? new BytesRef(binaryKeyString(bytes)) : new BytesRef(bytes);
        }
        return new BytesRef(key.toString());
    }

    /** Binary keys are ip columns: render the address string like classic ip terms. */
    static String binaryKeyString(byte[] bytes) {
        try {
            return NetworkAddress.format(InetAddress.getByAddress(bytes));
        } catch (UnknownHostException e) {
            // Not a 4/16-byte address; fall back to a printable, deterministic form.
            return Base64.getEncoder().encodeToString(bytes);
        }
    }
}
