/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.index;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class CompressedIndex extends Index implements Writeable, ToXContentObject {

    public static final CompressedIndex[] EMPTY_ARRAY = new CompressedIndex[0];
    private static final String INDEX_UUID_KEY = "index_uuid";
    private static final String INDEX_ORDINAL_KEY = "index_name";
    public static final String UNKNOWN_INDEX_NAME = "_unknown_";

    private static final ObjectParser<CompressedIndex.Builder, Void> INDEX_PARSER = new ObjectParser<>(
        "index",
        CompressedIndex.Builder::new
    );
    static {
        // INDEX_PARSER.declareString(CompressedIndex.Builder::name, new ParseField(INDEX_ORDINAL_KEY));
        INDEX_PARSER.declareString(CompressedIndex.Builder::uuid, new ParseField(INDEX_UUID_KEY));
    }

    private final int ordinal;
    private final String uuid;

    /**
     * Creates a new Index instance with name and unique identifier
     *
     * @param ordinal the name of the index
     * @param uuid the unique identifier of the index
     * @throws NullPointerException if either name or uuid are null
     */
    public CompressedIndex(int ordinal, String uuid) {
        super();
        this.ordinal = Objects.requireNonNull(ordinal);
        this.uuid = Objects.requireNonNull(uuid);
    }

    /**
     * Creates a new Index instance from a {@link StreamInput}.
     * Reads the name and unique identifier from the stream.
     *
     * @param in the stream to read from
     * @throws IOException if an error occurs while reading from the stream
     * @see #writeTo(StreamOutput)
     */
    public CompressedIndex(StreamInput in) throws IOException {
        super();
        this.ordinal = in.readVInt();
        this.uuid = in.readString();
    }

    /**
     * Gets the name of the index.
     *
     * @return the name of the index.
     */
    public int getOrdinal() {
        return this.ordinal;
    }

    /**
     * Gets the unique identifier of the index.
     *
     * @return the unique identifier of the index. "_na_" if {@link Strings#UNKNOWN_UUID_VALUE}.
     */
    public String getUUID() {
        return uuid;
    }

    public String getName() {
        OrdinalIndexMap ordinalIndexMap = OrdinalIndexMap.getInstance();
        return ordinalIndexMap.getOrdinalIndex(getOrdinal());
    }

    public Index getBaseObject() {
        return new Index(OrdinalIndexMap.getInstance().getOrdinalIndex(getOrdinal()), getUUID());
    }

    /**
     * Returns either the name and unique identifier of the index
     * or only the name if the uuid is {@link Strings#UNKNOWN_UUID_VALUE}.
     * <p>
     * If we have a uuid we put it in the toString so it'll show up in logs
     * which is useful as more and more things use the uuid rather
     * than the name as the lookup key for the index.
     *
     * @return {@code "[name/uuid]"} or {@code "[name]"}
     */
    @Override
    public String toString() {
        if (Strings.UNKNOWN_UUID_VALUE.equals(uuid)) {
            return "[" + ordinal + "]";
        }
        return "[" + ordinal + "/" + uuid + "]";
    }

    /**
     * Checks if this index is the same as another index by comparing the name and unique identifier.
     * If both uuid are {@link Strings#UNKNOWN_UUID_VALUE} then only the name is compared.
     *
     * @param o the index to compare to
     * @return true if the name and unique identifier are the same, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompressedIndex index1 = (CompressedIndex) o;
        return uuid.equals(index1.uuid) && ordinal == index1.getOrdinal(); // allow for _na_ uuid
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + uuid.hashCode();
        return result;
    }

    /** Writes the name and unique identifier to the {@link StreamOutput}
     *
     * @param out The stream to write to
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(ordinal);
        out.writeString(uuid);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_ORDINAL_KEY, ordinal);
        builder.field(INDEX_UUID_KEY, uuid);
        return builder.endObject();
    }

    public static CompressedIndex fromXContent(final XContentParser parser) throws IOException {
        return INDEX_PARSER.parse(parser, null).build();
    }

    /**
     * Builder for Index objects.  Used by ObjectParser instances only.
     *
     * @opensearch.internal
     */
    private static final class Builder {
        private int ordinal;
        private String uuid;

        public void ordinal(final int ordinal) {
            this.ordinal = ordinal;
        }

        public void uuid(final String uuid) {
            this.uuid = uuid;
        }

        public CompressedIndex build() {
            return new CompressedIndex(ordinal, uuid);
        }
    }
}
