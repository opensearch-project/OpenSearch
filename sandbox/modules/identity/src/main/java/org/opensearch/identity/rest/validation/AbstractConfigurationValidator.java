/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.validation;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.identity.DefaultObjectMapper;

public abstract class AbstractConfigurationValidator {

    JsonFactory factory = new JsonFactory();

    /* public for testing */
    public final static String INVALID_KEYS_KEY = "invalid_keys";

    /* public for testing */
    public final static String MISSING_MANDATORY_KEYS_KEY = "missing_mandatory_keys";

    /* public for testing */
    public final static String MISSING_MANDATORY_OR_KEYS_KEY = "specify_one_of";

    protected final Logger log = LogManager.getLogger(this.getClass());

    /** Define the various keys for this validator */
    protected final Map<String, DataType> allowedKeys = new HashMap<>();

    protected final Set<String> mandatoryKeys = new HashSet<>();

    protected final Set<String> mandatoryOrKeys = new HashSet<>();

    protected final Map<String, String> wrongDatatypes = new HashMap<>();

    /** Contain errorneous keys */
    protected final Set<String> missingMandatoryKeys = new HashSet<>();

    protected final Set<String> invalidKeys = new HashSet<>();

    protected final Set<String> missingMandatoryOrKeys = new HashSet<>();

    /** The error type */
    protected ErrorType errorType = ErrorType.NONE;

    /** Behaviour regarding payload */
    protected boolean payloadMandatory = false;

    protected boolean payloadAllowed = true;

    protected final Method method;

    protected final BytesReference content;

    protected final Settings opensearchSettings;

    protected final RestRequest request;

    protected final Object[] param;

    private JsonNode contentAsNode;

    public AbstractConfigurationValidator(
        final RestRequest request,
        final BytesReference ref,
        final Settings opensearchSettings,
        Object... param
    ) {
        this.content = ref;
        this.method = request.method();
        this.opensearchSettings = opensearchSettings;
        this.request = request;
        this.param = param;
    }

    public JsonNode getContentAsNode() {
        return contentAsNode;
    }

    /**
     *
     * @return false if validation fails
     */
    public boolean validate() {
        // no payload for DELETE and GET requests
        if (method.equals(Method.DELETE) || method.equals(Method.GET)) {
            return true;
        }

        if (this.payloadMandatory && content.length() == 0) {
            this.errorType = ErrorType.PAYLOAD_MANDATORY;
            return false;
        }

        if (!this.payloadMandatory && content.length() == 0) {
            return true;
        }

        if (this.payloadMandatory && content.length() > 0) {

            try {
                if (DefaultObjectMapper.readTree(content.utf8ToString()).size() == 0) {
                    this.errorType = ErrorType.PAYLOAD_MANDATORY;
                    return false;
                }

            } catch (IOException e) {
                log.error(ErrorType.BODY_NOT_PARSEABLE.toString(), e);
                this.errorType = ErrorType.BODY_NOT_PARSEABLE;
                return false;
            }
        }

        if (!this.payloadAllowed && content.length() > 0) {
            this.errorType = ErrorType.PAYLOAD_NOT_ALLOWED;
            return false;
        }

        // try to parse payload
        Set<String> requested = new HashSet<String>();
        try {
            contentAsNode = DefaultObjectMapper.readTree(content.utf8ToString());
            Set<String> fieldNames = new HashSet<>();
            contentAsNode.fieldNames().forEachRemaining(fieldNames::add);
            requested.addAll(fieldNames);
        } catch (Exception e) {
            log.error(ErrorType.BODY_NOT_PARSEABLE.toString(), e);
            this.errorType = ErrorType.BODY_NOT_PARSEABLE;
            return false;
        }

        // mandatory settings, one of ...
        if (Collections.disjoint(requested, mandatoryOrKeys)) {
            this.missingMandatoryOrKeys.addAll(mandatoryOrKeys);
        }

        // mandatory settings
        Set<String> mandatory = new HashSet<>(mandatoryKeys);
        mandatory.removeAll(requested);
        missingMandatoryKeys.addAll(mandatory);

        // invalid settings
        Set<String> allowed = new HashSet<>(allowedKeys.keySet());
        requested.removeAll(allowed);
        this.invalidKeys.addAll(requested);
        boolean valid = missingMandatoryKeys.isEmpty() && invalidKeys.isEmpty() && missingMandatoryOrKeys.isEmpty();
        if (!valid) {
            this.errorType = ErrorType.INVALID_CONFIGURATION;
        }

        // check types
        try {
            if (!checkDatatypes()) {
                this.errorType = ErrorType.WRONG_DATATYPE;
                return false;
            }
        } catch (Exception e) {
            log.error(ErrorType.BODY_NOT_PARSEABLE.toString(), e);
            this.errorType = ErrorType.BODY_NOT_PARSEABLE;
            return false;
        }

        // null element in the values of all the possible keys with DataType as ARRAY
        for (Entry<String, DataType> allowedKey : allowedKeys.entrySet()) {
            JsonNode value = contentAsNode.get(allowedKey.getKey());
            if (value != null) {
                if (hasNullArrayElement(value)) {
                    this.errorType = ErrorType.NULL_ARRAY_ELEMENT;
                    return false;
                }
            }
        }
        return valid;
    }

    private boolean checkDatatypes() throws Exception {
        String contentAsJson = XContentHelper.convertToJson(content, false, XContentType.JSON);
        try (JsonParser parser = factory.createParser(contentAsJson)) {
            JsonToken token = null;
            while ((token = parser.nextToken()) != null) {
                if (token.equals(JsonToken.FIELD_NAME)) {
                    String currentName = parser.getCurrentName();
                    DataType dataType = allowedKeys.get(currentName);
                    if (dataType != null) {
                        JsonToken valueToken = parser.nextToken();
                        switch (dataType) {
                            case STRING:
                                if (!valueToken.equals(JsonToken.VALUE_STRING)) {
                                    wrongDatatypes.put(currentName, "String expected");
                                }
                                break;
                            case ARRAY:
                                if (!valueToken.equals(JsonToken.START_ARRAY) && !valueToken.equals(JsonToken.END_ARRAY)) {
                                    wrongDatatypes.put(currentName, "Array expected");
                                }
                                break;
                            case OBJECT:
                                if (!valueToken.equals(JsonToken.START_OBJECT) && !valueToken.equals(JsonToken.END_OBJECT)) {
                                    wrongDatatypes.put(currentName, "Object expected");
                                }
                                break;
                        }
                    }
                }
            }
            return wrongDatatypes.isEmpty();
        }
    }

    public XContentBuilder errorsAsXContent(RestChannel channel) {
        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            switch (this.errorType) {
                case NONE:
                    builder.field("status", "error");
                    builder.field("reason", errorType.getMessage());
                    break;
                case INVALID_CONFIGURATION:
                    builder.field("status", "error");
                    builder.field("reason", ErrorType.INVALID_CONFIGURATION.getMessage());
                    addErrorMessage(builder, INVALID_KEYS_KEY, invalidKeys);
                    addErrorMessage(builder, MISSING_MANDATORY_KEYS_KEY, missingMandatoryKeys);
                    addErrorMessage(builder, MISSING_MANDATORY_OR_KEYS_KEY, missingMandatoryKeys);
                    break;
                case INVALID_PASSWORD:
                    builder.field("status", "error");
                    // builder.field("reason", opensearchSettings.get(ConfigConstants.SECURITY_RESTAPI_PASSWORD_VALIDATION_ERROR_MESSAGE,
                    // "Password does not match minimum criteria"));
                    break;
                case WRONG_DATATYPE:
                    builder.field("status", "error");
                    builder.field("reason", ErrorType.WRONG_DATATYPE.getMessage());
                    for (Entry<String, String> entry : wrongDatatypes.entrySet()) {
                        builder.field(entry.getKey(), entry.getValue());
                    }
                    break;
                case NULL_ARRAY_ELEMENT:
                    builder.field("status", "error");
                    builder.field("reason", ErrorType.NULL_ARRAY_ELEMENT.getMessage());
                    break;
                default:
                    builder.field("status", "error");
                    builder.field("reason", errorType.getMessage());

            }
            builder.endObject();
            return builder;
        } catch (IOException ex) {
            log.error("Cannot build error settings", ex);
            return null;
        }
    }

    private void addErrorMessage(final XContentBuilder builder, final String message, final Set<String> keys) throws IOException {
        if (!keys.isEmpty()) {
            builder.startObject(message);
            builder.field("keys", String.join(",", keys.toArray(new String[0])));
            builder.endObject();
        }
    }

    public static enum DataType {
        STRING,
        ARRAY,
        OBJECT,
        BOOLEAN;
    }

    public static enum ErrorType {
        NONE("ok"),
        INVALID_CONFIGURATION("Invalid configuration"),
        INVALID_PASSWORD("Invalid password"),
        WRONG_DATATYPE("Wrong datatype"),
        BODY_NOT_PARSEABLE("Could not parse content of request."),
        PAYLOAD_NOT_ALLOWED("Request body not allowed for this action."),
        PAYLOAD_MANDATORY("Request body required for this action."),
        IDENTITY_NOT_INITIALIZED("Identity index not initialized"),
        NULL_ARRAY_ELEMENT("`null` is not allowed as json array element");

        private String message;

        private ErrorType(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    protected final boolean hasParams() {
        return param != null && param.length > 0;
    }

    private boolean hasNullArrayElement(JsonNode node) {
        for (JsonNode element : node) {
            if (element.isNull()) {
                if (node.isArray()) {
                    return true;
                }
            } else if (element.isContainerNode()) {
                if (hasNullArrayElement(element)) {
                    return true;
                }
            }
        }
        return false;
    }
}
