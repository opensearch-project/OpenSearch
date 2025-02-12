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

package org.opensearch.index.engine;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.index.translog.Translog;

import java.util.Objects;

/**
 * Encapsulates an Index Version in the translog
 *
 * @opensearch.internal
 */
final class IndexVersionValue extends VersionValue {

    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IndexVersionValue.class);
    private static final long TRANSLOG_LOC_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Translog.Location.class);

    private final Translog.Location translogLocation;

    IndexVersionValue(Translog.Location translogLocation, long version, long seqNo, long term) {
        super(version, seqNo, term);
        this.translogLocation = translogLocation;
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED + (translogLocation == null ? 0L : TRANSLOG_LOC_RAM_BYTES_USED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        IndexVersionValue that = (IndexVersionValue) o;
        return Objects.equals(translogLocation, that.translogLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), translogLocation);
    }

    @Override
    public String toString() {
        return "IndexVersionValue{" + "version=" + version + ", seqNo=" + seqNo + ", term=" + term + ", location=" + translogLocation + '}';
    }

    @Override
    public Translog.Location getLocation() {
        return translogLocation;
    }
}
