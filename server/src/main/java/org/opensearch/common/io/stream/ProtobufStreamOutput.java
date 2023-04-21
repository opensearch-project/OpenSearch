/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common.io.stream;

import com.google.protobuf.CodedOutputStream;

import java.io.IOException;
import java.util.Map;

import org.opensearch.Version;
import org.opensearch.common.Nullable;

public class ProtobufStreamOutput {

    private Version version = Version.CURRENT;

    /**
     * The version of the node on the other side of this stream.
     */
    public Version getVersion() {
        return this.version;
    }

    /**
     * Set the version of the node on the other side of this stream.
     */
    public void setVersion(Version version) {
        this.version = version;
    }

    /**
     * Write a {@link Map} of {@code K}-type keys to {@code V}-type.
     * <pre><code>
     * Map&lt;String, String&gt; map = ...;
     * out.writeMap(map, StreamOutput::writeString, StreamOutput::writeString);
     * </code></pre>
     *
     * @param keyWriter The key writer
     * @param valueWriter The value writer
     */
    public final <K, V> void writeMap(final Map<K, V> map, final ProtobufWriteable.Writer<K> keyWriter, final ProtobufWriteable.Writer<V> valueWriter, CodedOutputStream out) throws IOException {
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            keyWriter.write(out, entry.getKey());
            valueWriter.write(out, entry.getValue());
        }
    }

    public void writeOptionalWriteable(@Nullable ProtobufWriteable writeable, CodedOutputStream out) throws IOException {
        if (writeable != null) {
            out.writeBool(1, true);
            writeable.writeTo(out);
        } else {
            out.writeBool(1, false);
        }
    }

}