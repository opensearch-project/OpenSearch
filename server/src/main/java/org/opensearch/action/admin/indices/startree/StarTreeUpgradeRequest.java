/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.startree;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;

import java.io.IOException;
import java.util.Map;

/**
 * A request to upgrade an existing index by adding star tree configuration.
 * Carries raw star tree config bytes to be parsed by {@code StarTreeMapper} during the mapping
 * merge phase in {@link TransportStarTreeUpgradeAction}. This ensures the upgrade API automatically
 * supports any new features added to the mapper without requiring parallel parsing logic.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeUpgradeRequest extends BroadcastRequest<StarTreeUpgradeRequest> {

    private static final String STAR_TREE = "star_tree";
    private static final String NAME = "name";

    /** Raw star tree config JSON (the content of the "star_tree" key from the request body). */
    private final BytesReference starTreeConfigSource;

    /** Star tree field name extracted minimally for mapping construction. */
    private final String starTreeFieldName;

    /**
     * Constructs a star tree upgrade request with raw config bytes.
     *
     * @param indices the target indices to upgrade
     * @param starTreeFieldName the star tree field name
     * @param starTreeConfigSource the raw JSON bytes of the star tree config
     */
    public StarTreeUpgradeRequest(String[] indices, String starTreeFieldName, BytesReference starTreeConfigSource) {
        super(indices);
        this.starTreeFieldName = starTreeFieldName;
        this.starTreeConfigSource = starTreeConfigSource;
    }

    /**
     * Convenience constructor for tests and internal callers that already have a parsed StarTreeField.
     * Serializes the StarTreeField to raw bytes for transport.
     *
     * @param indices the target indices to upgrade
     * @param starTreeField the star tree field configuration
     */
    public StarTreeUpgradeRequest(String[] indices, org.opensearch.index.compositeindex.datacube.startree.StarTreeField starTreeField)
        throws IOException {
        super(indices);
        this.starTreeFieldName = starTreeField.getName();
        try (org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            starTreeField.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            this.starTreeConfigSource = org.opensearch.core.common.bytes.BytesReference.bytes(builder);
        }
    }

    /**
     * Constructs a star tree upgrade request from a stream input for transport deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public StarTreeUpgradeRequest(StreamInput in) throws IOException {
        super(in);
        this.starTreeFieldName = in.readString();
        this.starTreeConfigSource = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(starTreeFieldName);
        out.writeBytesReference(starTreeConfigSource);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (starTreeConfigSource == null || starTreeConfigSource.length() == 0) {
            validationException = ValidateActions.addValidationError("star tree field configuration is required", validationException);
        }
        if (starTreeFieldName == null || starTreeFieldName.isEmpty()) {
            validationException = ValidateActions.addValidationError("star tree field name is required", validationException);
        }
        if (indices == null || indices.length == 0) {
            validationException = ValidateActions.addValidationError("index is required", validationException);
        }
        return validationException;
    }

    /**
     * Returns the star tree field name.
     */
    public String getStarTreeFieldName() {
        return starTreeFieldName;
    }

    /**
     * Returns the raw star tree config source bytes.
     * This is the content of the "star_tree" key from the user's request body.
     */
    public BytesReference getStarTreeConfigSource() {
        return starTreeConfigSource;
    }

    /**
     * Returns the star tree config as a Map for use in building the mapping source.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getStarTreeConfigAsMap() throws IOException {
        return XContentHelper.convertToMap(starTreeConfigSource, false, org.opensearch.core.xcontent.MediaTypeRegistry.JSON).v2();
    }

    /**
     * Parses the star tree field name and raw config bytes from an XContent request body.
     * Only extracts the "name" field for routing; all other parsing is deferred to
     * {@code StarTreeMapper} during the mapping merge phase.
     *
     * @param content the request body bytes
     * @param mediaType the media type of the content
     * @return a StarTreeUpgradeRequest with raw config bytes
     * @throws IOException if parsing fails
     */
    @SuppressWarnings("unchecked")
    public static StarTreeUpgradeRequest fromRequestBody(String[] indices, BytesReference content, MediaType mediaType) throws IOException {
        Map<String, Object> bodyMap = XContentHelper.convertToMap(content, false, mediaType).v2();
        Object starTreeObj = bodyMap.get(STAR_TREE);
        if (starTreeObj == null) {
            throw new IllegalArgumentException("request body must contain a [star_tree] configuration");
        }
        if (starTreeObj instanceof Map == false) {
            throw new IllegalArgumentException("[star_tree] must be an object");
        }
        Map<String, Object> starTreeMap = (Map<String, Object>) starTreeObj;
        String name = (String) starTreeMap.get(NAME);
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("[name] is required for star tree configuration");
        }

        // Serialize the star_tree section back to bytes for transport
        BytesReference configBytes;
        try (org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.map(starTreeMap);
            configBytes = BytesReference.bytes(builder);
        }

        return new StarTreeUpgradeRequest(indices, name, configBytes);
    }

    @Override
    public String toString() {
        return "StarTreeUpgradeRequest{" + "starTreeFieldName=" + starTreeFieldName + '}';
    }
}
