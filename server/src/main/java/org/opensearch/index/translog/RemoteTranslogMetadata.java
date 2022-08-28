/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.lucene.util.SetOnce;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class RemoteTranslogMetadata implements Writeable, Comparable<RemoteTranslogMetadata> {

    private final long primaryTerm;

    private final long generation;

    private final long minTranslogGeneration;

    private final long timeStamp;

    private final SetOnce<Map<String, Object>> generationToPrimaryTermMapper = new SetOnce<>();

    private static final String METADATA_SEPARATOR = "_";

    public RemoteTranslogMetadata(long primaryTerm, long generation, long minTranslogGeneration) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.timeStamp = System.currentTimeMillis();
    }

    RemoteTranslogMetadata(StreamInput in) throws IOException {
        this.primaryTerm = in.readLong();
        this.generation = in.readLong();
        this.minTranslogGeneration = in.readLong();
        this.timeStamp = in.readLong();
        this.generationToPrimaryTermMapper.set(in.readMap());
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getGeneration() {
        return generation;
    }

    public long getMinTranslogGeneration() {
        return minTranslogGeneration;
    }

    public void setGenerationToPrimaryTermMapper(Map<String, Object> generationToPrimaryTermMap) {
        generationToPrimaryTermMapper.set(generationToPrimaryTermMap);
    }

    public String getMetadataFileName() {
        return String.join(
            METADATA_SEPARATOR,
            Arrays.asList(String.valueOf(primaryTerm), String.valueOf(generation), String.valueOf(timeStamp))
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, generation, timeStamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteTranslogMetadata other = (RemoteTranslogMetadata) o;
        return Objects.equals(this.primaryTerm, other.primaryTerm)
            && Objects.equals(this.generation, other.generation)
            && Objects.equals(this.timeStamp, other.timeStamp);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(primaryTerm);
        out.writeLong(generation);
        out.writeLong(minTranslogGeneration);
        out.writeLong(timeStamp);
        out.writeMap(generationToPrimaryTermMapper.get());
    }

    @Override
    public int compareTo(RemoteTranslogMetadata o) {
        return -1;
    }
}
