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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An extension to {@link BytesReference} that requires releasing its content. This
 * class exists to make it explicit when a bytes reference needs to be released, and when not.
 *
 * @opensearch.internal
 */
public final class ReleasableBytesReference implements Releasable, BytesReference {

    public static final Releasable NO_OP = () -> {};
    private final BytesReference delegate;
    private final RefCountedReleasable<Releasable> refCounted;

    public ReleasableBytesReference(BytesReference delegate, Releasable releasable) {
        this.delegate = delegate;
        this.refCounted = new RefCountedReleasable<>("bytes-reference", releasable, releasable::close);
    }

    private ReleasableBytesReference(BytesReference delegate, RefCountedReleasable<Releasable> refCounted) {
        this.delegate = delegate;
        this.refCounted = refCounted;
        refCounted.incRef();
    }

    public static ReleasableBytesReference wrap(BytesReference reference) {
        return new ReleasableBytesReference(reference, NO_OP);
    }

    public int refCount() {
        return refCounted.refCount();
    }

    public ReleasableBytesReference retain() {
        refCounted.incRef();
        return this;
    }

    public ReleasableBytesReference retainedSlice(int from, int length) {
        return new ReleasableBytesReference(delegate.slice(from, length), refCounted);
    }

    @Override
    public void close() {
        refCounted.close();
    }

    @Override
    public byte get(int index) {
        return delegate.get(index);
    }

    @Override
    public int getInt(int index) {
        return delegate.getInt(index);
    }

    @Override
    public int indexOf(byte marker, int from) {
        return delegate.indexOf(marker, from);
    }

    @Override
    public int length() {
        return delegate.length();
    }

    @Override
    public BytesReference slice(int from, int length) {
        return delegate.slice(from, length);
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed();
    }

    @Override
    public StreamInput streamInput() throws IOException {
        return delegate.streamInput();
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        delegate.writeTo(os);
    }

    @Override
    public String utf8ToString() {
        return delegate.utf8ToString();
    }

    @Override
    public BytesRef toBytesRef() {
        return delegate.toBytesRef();
    }

    @Override
    public BytesRefIterator iterator() {
        return delegate.iterator();
    }

    @Override
    public int compareTo(BytesReference o) {
        return delegate.compareTo(o);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return delegate.toXContent(builder, params);
    }

    @Override
    public boolean isFragment() {
        return delegate.isFragment();
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
