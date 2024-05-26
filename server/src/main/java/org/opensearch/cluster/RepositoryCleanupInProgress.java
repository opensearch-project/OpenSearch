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
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.repositories.RepositoryOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
                if (params.param(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API).equals(Metadata.CONTEXT_MODE_GATEWAY)) {
                    builder.field("repository_state_id", entry.repositoryStateId);
                } // else we don't serialize it
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    public static RepositoryCleanupInProgress fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureFieldName(parser, parser.currentToken(), TYPE);
        parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<Entry> entries = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            String repository = null;
            long repositoryStateId = -1L;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                String currentFieldName = parser.currentName();
                parser.nextToken();
                if ("repository".equals(currentFieldName)) {
                    repository = parser.text();
                } else if ("repository_state_id".equals(currentFieldName)) {
                    // only XContent parsed with {@link Metadata.CONTEXT_MODE_GATEWAY} will have the repository state id and can be deserialized
                    repositoryStateId = parser.longValue();
                } else {
                    throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                }
            }
            entries.add(new Entry(repository, repositoryStateId));
        }
        return new RepositoryCleanupInProgress(entries);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return LegacyESVersion.fromId(7040099);
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
        public String toString() {
            return "{" + repository + '}' + '{' + repositoryStateId + '}';
        }
    }
}
