package org.opensearch.action;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.util.Map;
import java.util.regex.Pattern;

import org.opensearch.common.ValidationException;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * TODO: This validation should be associated with REST requests to ensure the parameter is from the URL
 * 
 * Context: If all of the resource information is in the URL, caching which can include responses or authorization are trivial
 */
@ExperimentalApi
public interface ResourceRequest {

    static final Pattern ALLOWED_KEY_PATTERN = Pattern.compile("[a-zA-Z_-]+");
    static final Pattern ALLOWED_VALUE_PATTERN = Pattern.compile("[a-zA-Z_-]+");

    /**
     * Extracts resource key value pairs from the request parameters
     * Note; these resource must be part of the 
     */
    Map<String, String> getResourceIds();

    public static ActionRequestValidationException validResourceIds(final ResourceRequest resourceRequest, final ActionRequestValidationException validationException) {
        resourceRequest.getResourceIds().entrySet().forEach(entry -> {
            if (!ALLOWED_KEY_PATTERN.matcher(entry.getKey()).matches()) {
                addValidationError("Unsupported resource key is not supported; key: " + entry.getKey() + " value: "  + entry.getValue() + " allowed pattern " + ALLOWED_VALUE_PATTERN.pattern(), validationException);
            }

            if (!ALLOWED_VALUE_PATTERN.matcher(entry.getKey()).matches()) {
                addValidationError("Unsupported resource value is not supported; key: " + entry.getKey() + " value: "  + entry.getValue() + " allowed pattern " + ALLOWED_VALUE_PATTERN.pattern(), validationException);
            }
        });

        return validationException;
    }
}
