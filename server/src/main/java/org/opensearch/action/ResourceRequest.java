package org.opensearch.action;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.util.Map;
import java.util.regex.Pattern;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * TODO: This validation should be associated with REST requests to ensure the parameter is from the URL
 * Note; this should work well in RestHandlers
 * 
 * Context: If all of the resource information is in the URL, caching which can include responses or authorization are trivial
 */
@ExperimentalApi
public interface ResourceRequest {

    static final Pattern ALLOWED_RESOURCE_NAME_PATTERN = Pattern.compile("[a-zA-Z_-]+");
    /** 
     * Don't allow wildcard patterns
     * this has large impact to perf and cachability */
    static final Pattern ALLOWED_RESOURCE_ID_PATTERN = Pattern.compile("[a-zA-Z_-]+");

    /**
     * Validates the resource type and id pairs are in an allowed format
     */
    Map<String, String> getResourceTypeAndIds();

    public static ActionRequestValidationException validResourceIds(final ResourceRequest resourceRequest, final ActionRequestValidationException validationException) {
        resourceRequest.getResourceTypeAndIds().entrySet().forEach(entry -> {
            if (!ALLOWED_RESOURCE_NAME_PATTERN.matcher(entry.getKey()).matches()) {
                addValidationError("Unsupported resource key is not supported; key: " + entry.getKey() + " value: "  + entry.getValue() + " allowed pattern " + ALLOWED_RESOURCE_NAME_PATTERN.pattern(), validationException);
            }

            if (!ALLOWED_RESOURCE_ID_PATTERN.matcher(entry.getKey()).matches()) {
                addValidationError("Unsupported resource value is not supported; key: " + entry.getKey() + " value: "  + entry.getValue() + " allowed pattern " + ALLOWED_RESOURCE_ID_PATTERN.pattern(), validationException);
            }
        });

        return validationException;
    }
}
