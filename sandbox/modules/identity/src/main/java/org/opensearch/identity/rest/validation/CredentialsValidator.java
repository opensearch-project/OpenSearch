/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.validation;

import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.NotXContentException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.utils.Utils;
import org.opensearch.rest.RestRequest;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Validator for validating password and hash present in the payload
 */
public class CredentialsValidator extends AbstractConfigurationValidator {

    public CredentialsValidator(final RestRequest request, BytesReference ref, final Settings opensearchSettings, Object... param) {
        super(request, ref, opensearchSettings, param);
        this.payloadMandatory = true;
        allowedKeys.put("hash", DataType.STRING);
        allowedKeys.put("password", DataType.STRING);
    }

    /**
     * Function to validate password in the content body.
     * @return true if validation is successful else false
     */
    @Override
    public boolean validate() {
        if (!super.validate()) {
            return false;
        }

        final String regex = this.opensearchSettings.get(ConfigConstants.IDENTITY_RESTAPI_PASSWORD_VALIDATION_REGEX, null);

        if ((request.method() == RestRequest.Method.PUT || request.method() == RestRequest.Method.PATCH)
            && this.content != null
            && this.content.length() > 1) {
            try {
                final Map<String, Object> contentAsMap = XContentHelper.convertToMap(this.content, false, XContentType.JSON).v2();
                String password = (String) contentAsMap.get("password");
                if (password != null) {
                    // Password is not allowed to be empty if present.
                    if (password.isEmpty()) {
                        this.errorType = ErrorType.INVALID_PASSWORD;
                        return false;
                    }

                    if (!Strings.isNullOrEmpty(regex)) {
                        // Password can be null for an existing user. Regex will validate password if present
                        if (!Pattern.compile("^" + regex + "$").matcher(password).matches()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Regex does not match password");
                            }
                            this.errorType = ErrorType.INVALID_PASSWORD;
                            return false;
                        }

                        final String username = Utils.coalesce(request.param("name"), hasParams() ? (String) param[0] : null);
                        final boolean isDebugEnabled = log.isDebugEnabled();

                        if (username == null || username.isEmpty()) {
                            if (isDebugEnabled) {
                                log.debug("Unable to validate username because no user is given");
                            }
                            return false;
                        }

                        if (username.toLowerCase().equals(password.toLowerCase())) {
                            if (isDebugEnabled) {
                                log.debug("Username must not match password");
                            }
                            this.errorType = ErrorType.INVALID_PASSWORD;
                            return false;
                        }
                    }
                }
            } catch (NotXContentException e) {
                // this.content is not valid json/yaml
                log.error("Invalid xContent: " + e, e);
                return false;
            }
        }
        return true;
    }
}
