/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Contains metadata about decommission attribute
 *
 * @opensearch.internal
 */
public class DecommissionAttributeMetadata extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "decommissionedAttribute";

    private final DecommissionAttribute decommissionAttribute;
    private DecommissionStatus status;
    private String requestID;
    public static final String attributeType = "awareness";

    /**
     * Constructs new decommission attribute metadata with given status
     *
     * @param decommissionAttribute attribute details
     * @param status                current status of the attribute decommission
     */
    public DecommissionAttributeMetadata(DecommissionAttribute decommissionAttribute, DecommissionStatus status, String requestId) {
        this.decommissionAttribute = decommissionAttribute;
        this.status = status;
        this.requestID = requestId;
    }

    /**
     * Constructs new decommission attribute metadata with status as {@link DecommissionStatus#INIT} and request id
     *
     * @param decommissionAttribute attribute details
     */
    public DecommissionAttributeMetadata(DecommissionAttribute decommissionAttribute, String requestID) {
        this(decommissionAttribute, DecommissionStatus.INIT, requestID);
    }

    /**
     * Returns the current decommissioned attribute
     *
     * @return decommissioned attributes
     */
    public DecommissionAttribute decommissionAttribute() {
        return this.decommissionAttribute;
    }

    /**
     * Returns the current status of the attribute decommission
     *
     * @return attribute type
     */
    public DecommissionStatus status() {
        return this.status;
    }

    /**
     * Returns the request id of the decommission
     *
     * @return request id
     */
    public String requestID() {
        return this.requestID;
    }

    /**
     * Returns instance of the metadata with updated status
     * @param newStatus status to be updated with
     */
    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized void validateNewStatus(DecommissionStatus newStatus) {
        // if the current status is the expected status already or new status is FAILED, we let the check pass
        if (newStatus.equals(status) || newStatus.equals(DecommissionStatus.FAILED)) {
            return;
        }
        // We don't expect that INIT will be new status, as it is registered only when starting the decommission action
        switch (newStatus) {
            case DRAINING:
                validateStatus(Set.of(DecommissionStatus.INIT), newStatus);
                break;
            case IN_PROGRESS:
                validateStatus(Set.of(DecommissionStatus.DRAINING, DecommissionStatus.INIT), newStatus);
                break;
            case SUCCESSFUL:
                validateStatus(Set.of(DecommissionStatus.IN_PROGRESS), newStatus);
                break;
            default:
                throw new IllegalArgumentException(
                    "illegal decommission status [" + newStatus.status() + "] requested for updating metadata"
                );
        }
    }

    private void validateStatus(Set<DecommissionStatus> expectedStatuses, DecommissionStatus next) {
        if (expectedStatuses.contains(status) == false) {
            assert false : "can't move decommission status to ["
                + next
                + "]. current status: ["
                + status
                + "] (allowed statuses ["
                + expectedStatuses
                + "])";
            throw new IllegalStateException(
                "can't move decommission status to [" + next + "]. current status: [" + status + "] (expected [" + expectedStatuses + "])"
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DecommissionAttributeMetadata that = (DecommissionAttributeMetadata) o;

        if (!status.equals(that.status)) return false;
        if (!requestID.equals(that.requestID)) return false;
        return decommissionAttribute.equals(that.decommissionAttribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeType, decommissionAttribute, status, requestID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_2_4_0;
    }

    public DecommissionAttributeMetadata(StreamInput in) throws IOException {
        this.decommissionAttribute = new DecommissionAttribute(in);
        this.status = DecommissionStatus.fromString(in.readString());
        this.requestID = in.readString();
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        decommissionAttribute.writeTo(out);
        out.writeString(status.status());
        out.writeString(requestID);
    }

    public static DecommissionAttributeMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        DecommissionAttribute decommissionAttribute = null;
        DecommissionStatus status = null;
        String requestID = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (attributeType.equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new OpenSearchParseException(
                            "failed to parse decommission attribute type [{}], expected object",
                            attributeType
                        );
                    }
                    token = parser.nextToken();
                    if (token != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            String fieldName = parser.currentName();
                            String value;
                            token = parser.nextToken();
                            if (token == XContentParser.Token.VALUE_STRING) {
                                value = parser.text();
                            } else {
                                throw new OpenSearchParseException(
                                    "failed to parse attribute [{}], expected string for attribute value",
                                    fieldName
                                );
                            }
                            decommissionAttribute = new DecommissionAttribute(fieldName, value);
                            parser.nextToken();
                        } else {
                            throw new OpenSearchParseException("failed to parse attribute type [{}], unexpected type", attributeType);
                        }
                    } else {
                        throw new OpenSearchParseException("failed to parse attribute type [{}]", attributeType);
                    }
                } else if ("status".equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new OpenSearchParseException(
                            "failed to parse status of decommissioning, expected string but found unknown type"
                        );
                    }
                    status = DecommissionStatus.fromString(parser.text());
                } else if ("requestID".equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new OpenSearchParseException(
                            "failed to parse status of decommissioning, expected string but found unknown type"
                        );
                    }
                    requestID = parser.text();
                } else {
                    throw new OpenSearchParseException(
                        "unknown field found [{}], failed to parse the decommission attribute",
                        currentFieldName
                    );
                }
            }
        }
        return new DecommissionAttributeMetadata(decommissionAttribute, status, requestID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        toXContent(decommissionAttribute, status, requestID, attributeType, builder, params);
        return builder;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    /**
     * @param decommissionAttribute decommission attribute
     * @param status                decommission  status
     * @param attributeType         attribute type
     * @param builder               XContent builder
     * @param params                serialization parameters
     */
    public static void toXContent(
        DecommissionAttribute decommissionAttribute,
        DecommissionStatus status,
        String requestID,
        String attributeType,
        XContentBuilder builder,
        ToXContent.Params params
    ) throws IOException {
        builder.startObject(attributeType);
        builder.field(decommissionAttribute.attributeName(), decommissionAttribute.attributeValue());
        builder.endObject();
        builder.field("status", status.status());
        builder.field("requestID", requestID);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }
}
