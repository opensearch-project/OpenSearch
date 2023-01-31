/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

public interface AuthorizationRequirements {

    String getRequestType();

    String[] getRequestParameters();

    Boolean isParametersFulfilled();

}

