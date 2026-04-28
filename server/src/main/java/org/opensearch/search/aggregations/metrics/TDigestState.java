/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;

/**
 * Extension of {@link com.tdunning.math.stats.TDigest} with custom serialization.
 *
 * @opensearch.internal
 */
public class TDigestState extends MergingDigest {

    private final double compression;

    public TDigestState(double compression) {
        super(compression);
        this.compression = compression;
    }

    private TDigestState(double compression, MergingDigest in) {
        super(compression);
        this.compression = compression;
        this.add(List.of(in));
    }

    @Override
    public double compression() {
        return compression;
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_3_4_0)) {
            out.writeDouble(state.compression);
            out.writeVInt(state.centroidCount());
            for (Centroid centroid : state.centroids()) {
                out.writeDouble(centroid.mean());
                out.writeVLong(centroid.count());
            }
        } else {
            int byteSize = state.byteSize();
            out.writeVInt(byteSize);
            ByteBuffer buf = ByteBuffer.allocate(byteSize);
            state.asBytes(buf);
            out.writeBytes(buf.array());
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_3_4_0)) {
            // In older versions TDigestState was based on AVLTreeDigest. Load centroids into this class, then add it to MergingDigest.
            double compression = in.readDouble();

            int n = in.readVInt();
            if (n <= 0) {
                return new TDigestState(compression);
            }
            AVLTreeDigest treeDigest = new AVLTreeDigest(compression);
            for (int i = 0; i < n; i++) {
                treeDigest.add(in.readDouble(), in.readVInt());
            }
            TDigestState state = new TDigestState(compression);
            state.add(List.of(treeDigest));
            return state;

        } else {
            // For MergingDigest, adding the original centroids in ascending order to a new, empty MergingDigest isn't guaranteed
            // to produce a MergingDigest whose centroids are exactly equal to the originals.
            // So, use the library's serialization code to ensure we get the exact same centroids, allowing us to compare with equals().
            // The AVLTreeDigest had the same limitation for equals() where it was only guaranteed to return true if the other object was
            // produced by de/serializing the object, so this should be fine.
            int byteSize = in.readVInt();
            byte[] bytes = new byte[byteSize];
            in.readBytes(bytes, 0, byteSize);
            MergingDigest mergingDigest = MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
            if (mergingDigest.centroids().isEmpty()) {
                return new TDigestState(mergingDigest.compression());
            }
            return new TDigestState(mergingDigest.compression(), mergingDigest);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof TDigestState == false) {
            return false;
        }
        TDigestState that = (TDigestState) obj;
        if (compression != that.compression) {
            return false;
        }
        Iterator<? extends Centroid> thisCentroids = centroids().iterator();
        Iterator<? extends Centroid> thatCentroids = that.centroids().iterator();
        while (thisCentroids.hasNext()) {
            if (thatCentroids.hasNext() == false) {
                return false;
            }
            Centroid thisNext = thisCentroids.next();
            Centroid thatNext = thatCentroids.next();
            if (thisNext.mean() != thatNext.mean() || thisNext.count() != thatNext.count()) {
                return false;
            }
        }
        return thatCentroids.hasNext() == false;
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + Double.hashCode(compression);
        for (Centroid centroid : centroids()) {
            h = 31 * h + Double.hashCode(centroid.mean());
            h = 31 * h + centroid.count();
        }
        return h;
    }
}
