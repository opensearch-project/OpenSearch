/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.index.fielddomain.FieldDomain;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request for publishing field-domain metadata to one concrete index.
 *
 * Producers should use {@link #fieldDomain(FieldDomain)} or {@link #fieldDomains(Collection)} so typed
 * {@link FieldDomain} objects are encoded through the central {@link IndexFieldDomainMetadata} codec. The transport
 * request carries the encoded map because {@link org.opensearch.cluster.metadata.IndexMetadata} stores custom metadata
 * as a flat {@code Map<String, String>}. This request intentionally targets an exact concrete {@link Index} instead of
 * implementing {@link IndicesRequest.Replaceable}; callers must not rewrite the target name without also preserving the
 * UUID identity guard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PutIndexFieldDomainsRequest extends AcknowledgedRequest<PutIndexFieldDomainsRequest> implements IndicesRequest {
    private Index targetIndex;
    private Map<String, String> fieldDomainCustomData = Map.of();

    /**
     * Deserializes the request from transport.
     */
    public PutIndexFieldDomainsRequest(StreamInput in) throws IOException {
        super(in);
        targetIndex = new Index(in);
        fieldDomainCustomData = Map.copyOf(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    /**
     * Creates a request for the exact target concrete index.
     */
    public PutIndexFieldDomainsRequest(Index targetIndex) {
        this.targetIndex = targetIndex;
    }

    /**
     * Returns the exact target concrete index.
     */
    public Index targetIndex() {
        return targetIndex;
    }

    /**
     * Sets the exact target concrete index.
     */
    public PutIndexFieldDomainsRequest targetIndex(Index targetIndex) {
        this.targetIndex = targetIndex;
        return this;
    }

    /**
     * Encodes and adds one field domain to this request.
     */
    public PutIndexFieldDomainsRequest fieldDomain(FieldDomain domain) {
        Objects.requireNonNull(domain, "domain must not be null");

        List<FieldDomain> domains = new ArrayList<>();
        if (fieldDomainCustomData.isEmpty() == false) {
            domains.addAll(IndexFieldDomainMetadata.getInstance().validateAndParseCustomData(fieldDomainCustomData));
        }
        domains.add(domain);
        return fieldDomains(domains);
    }

    /**
     * Encodes and sets field domains, replacing any field domains already configured on this request.
     */
    public PutIndexFieldDomainsRequest fieldDomains(Collection<? extends FieldDomain> domains) {
        fieldDomainCustomData = IndexFieldDomainMetadata.getInstance().toCustomData(domains);
        return this;
    }

    /**
     * Returns the encoded field-domain metadata carried by this request.
     */
    public Map<String, String> fieldDomainCustomData() {
        return fieldDomainCustomData;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (targetIndex == null) {
            validationException = addValidationError("target index is required", validationException);
        } else {
            if (targetIndex.getName().isBlank()) {
                validationException = addValidationError("target index name is required", validationException);
            }
            if (targetIndex.getUUID().isBlank() || Strings.UNKNOWN_UUID_VALUE.equals(targetIndex.getUUID())) {
                validationException = addValidationError("target index UUID is required", validationException);
            }
        }
        if (fieldDomainCustomData == null || fieldDomainCustomData.isEmpty()) {
            validationException = addValidationError("field domain metadata is required", validationException);
        } else {
            for (Map.Entry<String, String> entry : fieldDomainCustomData.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank()) {
                    validationException = addValidationError("field domain metadata keys must not be empty", validationException);
                }
                if (entry.getValue() == null) {
                    validationException = addValidationError("field domain metadata values must not be null", validationException);
                }
            }
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return targetIndex == null ? Strings.EMPTY_ARRAY : new String[] { targetIndex.getName() };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Objects.requireNonNull(targetIndex, "targetIndex must not be null").writeTo(out);
        out.writeMap(fieldDomainCustomData, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PutIndexFieldDomainsRequest that = (PutIndexFieldDomainsRequest) o;
        return Objects.equals(targetIndex, that.targetIndex)
            && Objects.equals(fieldDomainCustomData, that.fieldDomainCustomData)
            && Objects.equals(clusterManagerNodeTimeout, that.clusterManagerNodeTimeout)
            && Objects.equals(timeout, that.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetIndex, fieldDomainCustomData, clusterManagerNodeTimeout, timeout);
    }
}
