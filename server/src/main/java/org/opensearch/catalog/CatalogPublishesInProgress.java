/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Cluster-state record of in-flight catalog publishes. One {@link PublishEntry} per publish.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CatalogPublishesInProgress extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "catalog_publishes";

    public static final CatalogPublishesInProgress EMPTY = new CatalogPublishesInProgress(Collections.emptyList());

    private final List<PublishEntry> entries;

    public CatalogPublishesInProgress(List<PublishEntry> entries) {
        this.entries = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(entries, "entries")));
    }

    public CatalogPublishesInProgress(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<PublishEntry> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(new PublishEntry(in));
        }
        this.entries = Collections.unmodifiableList(list);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (PublishEntry entry : entries) {
            entry.writeTo(out);
        }
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    /**
     * Binary serialization is the canonical form; XContent is emit-only. Throws to fail loudly
     * if a caller tries to round-trip through JSON. TODO: implement when a consumer needs it.
     */
    public static CatalogPublishesInProgress fromXContent(XContentParser parser) throws IOException {
        throw new UnsupportedOperationException(
            "XContent parsing for [" + TYPE + "] is not implemented; use binary serialization"
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("entries");
        for (PublishEntry entry : entries) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumIndexCompatibilityVersion();
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    public List<PublishEntry> entries() {
        return entries;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Nullable
    public PublishEntry entry(String publishId) {
        for (PublishEntry entry : entries) {
            if (entry.publishId().equals(publishId)) {
                return entry;
            }
        }
        return null;
    }

    @Nullable
    public PublishEntry entryForIndex(String indexName) {
        for (PublishEntry entry : entries) {
            if (entry.indexName().equals(indexName)) {
                return entry;
            }
        }
        return null;
    }

    public CatalogPublishesInProgress withAddedEntry(PublishEntry entry) {
        if (entry(entry.publishId()) != null) {
            throw new IllegalStateException("entry [" + entry.publishId() + "] already exists");
        }
        List<PublishEntry> updated = new ArrayList<>(entries.size() + 1);
        updated.addAll(entries);
        updated.add(entry);
        return new CatalogPublishesInProgress(updated);
    }

    public CatalogPublishesInProgress withUpdatedEntry(PublishEntry updatedEntry) {
        List<PublishEntry> updated = new ArrayList<>(entries.size());
        boolean replaced = false;
        for (PublishEntry entry : entries) {
            if (entry.publishId().equals(updatedEntry.publishId())) {
                updated.add(updatedEntry);
                replaced = true;
            } else {
                updated.add(entry);
            }
        }
        if (!replaced) {
            throw new IllegalStateException("entry [" + updatedEntry.publishId() + "] not found");
        }
        return new CatalogPublishesInProgress(updated);
    }

    public CatalogPublishesInProgress withRemovedEntry(String publishId) {
        List<PublishEntry> updated = new ArrayList<>(entries.size());
        boolean removed = false;
        for (PublishEntry entry : entries) {
            if (entry.publishId().equals(publishId)) {
                removed = true;
                continue;
            }
            updated.add(entry);
        }
        if (!removed) {
            return this;
        }
        return new CatalogPublishesInProgress(updated);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return entries.equals(((CatalogPublishesInProgress) o).entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return "CatalogPublishesInProgress{size=" + entries.size() + "}";
    }
}
