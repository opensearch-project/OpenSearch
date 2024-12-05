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

package org.opensearch.cluster;

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.repositories.RepositoryOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Information passed during repository cleanup
 *
 * @opensearch.internal
 */
public final class RepositoryCleanupInProgress extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final RepositoryCleanupInProgress EMPTY = new RepositoryCleanupInProgress(Collections.emptyList());

    public static final String TYPE = "repository_cleanup";

    private final List<Entry> entries;

    public RepositoryCleanupInProgress(List<Entry> entries) {
        this.entries = entries;
    }

    RepositoryCleanupInProgress(StreamInput in) throws IOException {
        this.entries = in.readList(Entry::new);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    public static Entry startedEntry(String repository, long repositoryStateId) {
        return new Entry(repository, repositoryStateId);
    }

    public boolean hasCleanupInProgress() {
        // TODO: Should we allow parallelism across repositories here maybe?
        return entries.isEmpty() == false;
    }

    public List<Entry> entries() {
        return new ArrayList<>(entries);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TYPE);
        for (Entry entry : entries) {
            builder.startObject();
            {
                builder.field("repository", entry.repository);
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return LegacyESVersion.fromId(7040099);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RepositoryCleanupInProgress that = (RepositoryCleanupInProgress) o;
        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return 31 + entries.hashCode();
    }

    /**
     * Entry in the collection.
     *
     * @opensearch.internal
     */
    public static final class Entry implements Writeable, RepositoryOperation {

        private final String repository;

        private final long repositoryStateId;

        private Entry(StreamInput in) throws IOException {
            repository = in.readString();
            repositoryStateId = in.readLong();
        }

        public Entry(String repository, long repositoryStateId) {
            this.repository = repository;
            this.repositoryStateId = repositoryStateId;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }

        @Override
        public String repository() {
            return repository;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repository);
            out.writeLong(repositoryStateId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RepositoryCleanupInProgress.Entry that = (RepositoryCleanupInProgress.Entry) o;
            return repository.equals(that.repository) && repositoryStateId == that.repositoryStateId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(repository, repositoryStateId);
        }

        @Override
        public String toString() {
            return "{" + repository + '}' + '{' + repositoryStateId + '}';
        }
    }
}
