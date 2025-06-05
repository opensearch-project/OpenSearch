/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.RuleValidator;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A request for update Rule
 * Example request:
 * curl -XPUT "localhost:9200/_rules/{featureType}/{_id}" -H 'Content-Type: application/json' -d '
 * {
 *     "description": "description",
 *      "index_pattern": ["log*", "event*"],
 *      "workload_group": "dev_workload_group_id_2"
 * }'
 * @opensearch.experimental
 */
@ExperimentalApi
public class UpdateRuleRequest extends ActionRequest {
    private final String _id;
    private final String description;
    private final Map<Attribute, Set<String>> attributeMap;
    private final String featureValue;
    private final FeatureType featureType;

    /**
     * constructor for UpdateRuleRequest
     * @param _id - the rule id to update
     * @param description - the description to update
     * @param attributeMap - the attribute values to update
     * @param featureValue - the feature value to update
     * @param featureType - the feature type for the rule
     */
    public UpdateRuleRequest(
        String _id,
        String description,
        Map<Attribute, Set<String>> attributeMap,
        String featureValue,
        FeatureType featureType
    ) {
        this._id = _id;
        this.description = description;
        this.attributeMap = attributeMap;
        this.featureValue = featureValue;
        this.featureType = featureType;
    }

    /**
     * Constructs a UpdateRuleRequest from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public UpdateRuleRequest(StreamInput in) throws IOException {
        super(in);
        _id = in.readString();
        description = in.readOptionalString();
        featureType = FeatureType.from(in);
        attributeMap = in.readMap(i -> Attribute.from(i, featureType), i -> new HashSet<>(i.readStringList()));
        featureValue = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        RuleValidator validator = new RuleValidator(_id, description, attributeMap, featureValue, null, featureType);
        List<String> errors = validator.validateUpdatingRuleParams();
        if (!errors.isEmpty()) {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            validationException.addValidationErrors(errors);
            return validationException;
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(_id);
        out.writeOptionalString(description);
        featureType.writeTo(out);
        out.writeMap(attributeMap, (o, a) -> a.writeTo(o), StreamOutput::writeStringCollection);
        out.writeOptionalString(featureValue);
    }

    /**
     * id getter
     */
    public String get_id() {
        return _id;
    }

    /**
     * description getter
     */
    public String getDescription() {
        return description;
    }

    /**
     * attributeMap getter
     */
    public Map<Attribute, Set<String>> getAttributeMap() {
        return attributeMap;
    }

    /**
     * featureType getter
     */
    public FeatureType getFeatureType() {
        return featureType;
    }

    /**
     * featureValue getter
     */
    public String getFeatureValue() {
        return featureValue;
    }
}
