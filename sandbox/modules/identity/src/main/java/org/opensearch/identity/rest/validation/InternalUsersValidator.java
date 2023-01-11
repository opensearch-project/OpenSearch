/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.validation;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestRequest;

/**
 * Validator for Internal Users Api Action.
 */
public class InternalUsersValidator extends CredentialsValidator {

    public InternalUsersValidator(
        final RestRequest request,
        boolean isSuperAdmin,
        BytesReference ref,
        final Settings opensearchSettings,
        Object... param
    ) {
        super(request, ref, opensearchSettings, param);
        allowedKeys.put("backend_roles", DataType.ARRAY);
        allowedKeys.put("attributes", DataType.OBJECT);
        allowedKeys.put("description", DataType.STRING);
        allowedKeys.put("opendistro_security_roles", DataType.ARRAY);
        if (isSuperAdmin) allowedKeys.put("reserved", DataType.BOOLEAN);
    }
}
