/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.serializer;

import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ICacheKeySerializer<K> implements Serializer<ICacheKey<K>, byte[]> {

    public Serializer<K, byte[]> keySerializer;

    public ICacheKeySerializer(Serializer<K, byte[]> serializer) {
        this.keySerializer = serializer;
    }

    @Override
    public byte[] serialize(ICacheKey<K> object) {
        if (object == null || object.key == null || object.dimensions == null) {
            return null;
        }
        byte[] serializedKey = keySerializer.serialize(object.key);
        try {
            BytesStreamOutput os = new BytesStreamOutput();
            // First write the number of dimensions
            os.writeVInt(object.dimensions.size());
            for (CacheStatsDimension dim : object.dimensions) {
                dim.writeTo(os);
            }
            os.writeVInt(serializedKey.length); // ?? Is the read byte[] fn broken such that we have to do this?
            os.writeBytes(serializedKey); // TODO: Is this re-copying unnecessarily? Come back to this
            byte[] finalBytes = BytesReference.toBytes(os.bytes());
            return finalBytes;
        } catch (IOException e) {
            throw new OpenSearchException(e);
        }
    }

    @Override
    public ICacheKey<K> deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        List<CacheStatsDimension> dimensionList = new ArrayList<>();
        try {
            BytesStreamInput is = new BytesStreamInput(bytes, 0, bytes.length);
            int numDimensions = is.readVInt();
            for (int i = 0; i < numDimensions; i++) {
                dimensionList.add(new CacheStatsDimension(is));
            }

            int length = is.readVInt();
            byte[] serializedKey = new byte[length];
            is.readBytes(serializedKey, 0, length); // not sure why is.readByteArray doesn't work??
            return new ICacheKey<>(keySerializer.deserialize(serializedKey), dimensionList);
        } catch (IOException e) {
            throw new OpenSearchException(e);
        }
    }

    @Override
    public boolean equals(ICacheKey<K> object, byte[] bytes) {
        return Arrays.equals(serialize(object), bytes);
    }
}
