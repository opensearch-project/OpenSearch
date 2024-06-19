/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.metadata.QueryGroup.QueryGroupMode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.cluster.metadata.QueryGroup.ALLOWED_RESOURCES;
import static org.opensearch.plugin.wlm.CreateQueryGroupRequest.validateName;
import static org.opensearch.plugin.wlm.CreateQueryGroupRequest.validateMode;
import static org.opensearch.plugin.wlm.CreateQueryGroupRequest.validateUpdatedAtInMillis;

/**
 * A request for update QueryGroup
 *
 * @opensearch.internal
 */
public class UpdateQueryGroupRequest extends ActionRequest implements Writeable.Reader<UpdateQueryGroupRequest> {
    String name;
    Map<String, Object> resourceLimits;
    QueryGroupMode mode;
    long updatedAtInMillis;

    /**
     * Default constructor for UpdateQueryGroupRequest
     */
    public UpdateQueryGroupRequest() {}

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param queryGroup - A {@link QueryGroup} object
     */
    public UpdateQueryGroupRequest(QueryGroup queryGroup) {
        this.name = queryGroup.getName();
        this.mode = queryGroup.getMode();
        this.resourceLimits = queryGroup.getResourceLimits();
        this.updatedAtInMillis = queryGroup.getUpdatedAtInMillis();
        verifyUpdateQueryGroupRequest(name, mode, resourceLimits, updatedAtInMillis);
    }

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param name - QueryGroup name for UpdateQueryGroupRequest
     * @param mode - QueryGroup mode for UpdateQueryGroupRequest
     * @param resourceLimits - QueryGroup resourceLimits for UpdateQueryGroupRequest
     * @param updatedAtInMillis - QueryGroup updated time in millis for UpdateQueryGroupRequest
     */
    public UpdateQueryGroupRequest(String name, QueryGroupMode mode, Map<String, Object> resourceLimits, long updatedAtInMillis) {
        this.name = name;
        this.mode = mode;
        this.resourceLimits = resourceLimits;
        this.updatedAtInMillis = updatedAtInMillis;
        verifyUpdateQueryGroupRequest(name, mode, resourceLimits, updatedAtInMillis);
    }

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public UpdateQueryGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        if (in.readBoolean()) {
            resourceLimits = in.readMap();
        } else {
            resourceLimits = new HashMap<>();
        }
        if (in.readBoolean()) {
            mode = QueryGroupMode.fromName(in.readString());
        }
        updatedAtInMillis = in.readLong();
        verifyUpdateQueryGroupRequest(name, mode, resourceLimits, updatedAtInMillis);
    }

    /**
     * Verification for UpdateQueryGroupRequest
     * @param name - QueryGroup name for UpdateQueryGroupRequest
     * @param resourceLimits - QueryGroup resourceLimits for UpdateQueryGroupRequest
     * @param updatedAtInMillis - QueryGroup updated time in millis for UpdateQueryGroupRequest
     */
    private void verifyUpdateQueryGroupRequest(
        String name,
        QueryGroupMode mode,
        Map<String, Object> resourceLimits,
        long updatedAtInMillis
    ) {
        Objects.requireNonNull(name, "QueryGroup.name can't be null");
        Objects.requireNonNull(resourceLimits, "QueryGroup.resourceLimits can't be null");
        Objects.requireNonNull(updatedAtInMillis, "QueryGroup.updatedAtInMillis can't be null");

        validateName(name);
        validateMode(mode);
        validateResourceLimits(resourceLimits);
        validateUpdatedAtInMillis(updatedAtInMillis);
    }

    /**
     * Verification for resourceLimits
     * @param resourceLimits - QueryGroup resourceLimits for UpdateQueryGroupRequest
     */
    public static void validateResourceLimits(Map<String, Object> resourceLimits) {
        if (resourceLimits == null) return;
        for (Map.Entry<String, Object> resource : resourceLimits.entrySet()) {
            String resourceName = resource.getKey();
            Double threshold = (Double) resource.getValue();
            Objects.requireNonNull(resourceName, "resourceName can't be null");
            Objects.requireNonNull(threshold, "resource value can't be null");

            if (threshold < 0 || threshold > 1) {
                throw new IllegalArgumentException("Resource value should be between 0 and 1");
            }
            String str = String.valueOf(threshold);
            if (str.contains(".") && str.split("\\.")[1].length() > 2) {
                throw new IllegalArgumentException("Resource value should have at most two digits after the decimal point");
            }
            if (!ALLOWED_RESOURCES.contains(resourceName.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                    "resource has to be valid, valid resources are: " + ALLOWED_RESOURCES.stream().reduce((x, e) -> x + ", " + e).get()
                );
            }
        }
    }

    @Override
    public UpdateQueryGroupRequest read(StreamInput in) throws IOException {
        return new UpdateQueryGroupRequest(in);
    }

    /**
     * Generate a UpdateQueryGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     * @param name - name of the QueryGroup to be updated
     */
    public static UpdateQueryGroupRequest fromXContent(XContentParser parser, String name) throws IOException {
        while (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected start object but got a " + parser.currentToken());
        }

        XContentParser.Token token;
        String fieldName = "";
        QueryGroupMode mode = null;
        long updatedAtInMillis = Instant.now().getMillis();

        // Map to hold resources
        final Map<String, Object> resourceLimits_ = new HashMap<>();
        while ((token = parser.nextToken()) != null) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (fieldName.equals("mode")) {
                    mode = QueryGroupMode.fromName(parser.text());
                } else if (ALLOWED_RESOURCES.contains(fieldName)) {
                    resourceLimits_.put(fieldName, parser.doubleValue());
                } else {
                    throw new IllegalArgumentException("unrecognised [field=" + fieldName + " in QueryGroup");
                }
            }
        }
        return new UpdateQueryGroupRequest(name, mode, resourceLimits_, updatedAtInMillis);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * name getter
     */
    public String getName() {
        return name;
    }

    /**
     * name setter
     * @param name - name to be set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * ResourceLimits getter
     */
    public Map<String, Object> getResourceLimits() {
        return resourceLimits;
    }

    /**
     * ResourceLimits setter
     * @param resourceLimits - ResourceLimit to be set
     */
    public void setResourceLimits(Map<String, Object> resourceLimits) {
        this.resourceLimits = resourceLimits;
    }

    /**
     * Mode getter
     */
    public QueryGroupMode getMode() {
        return mode;
    }

    /**
     * Mode setter
     * @param mode - mode to be set
     */
    public void setMode(QueryGroupMode mode) {
        this.mode = mode;
    }

    /**
     * updatedAtInMillis getter
     */
    public long getUpdatedAtInMillis() {
        return updatedAtInMillis;
    }

    /**
     * updatedAtInMillis setter
     * @param updatedAtInMillis - updatedAtInMillis to be set
     */
    public void setUpdatedAtInMillis(long updatedAtInMillis) {
        this.updatedAtInMillis = updatedAtInMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(resourceLimits);
        }
        if (mode == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(mode.getName());
        }
        out.writeLong(updatedAtInMillis);
    }
}
