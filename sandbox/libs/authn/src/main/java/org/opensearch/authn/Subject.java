/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import java.security.Principal;
import java.util.List;
import java.util.Map;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 *
 * Used to authorize activities inside of the OpenSearch ecosystem.
 *
 * @opensearch.experimental
 */
public interface Subject {

    /**
     * Get the application-wide uniquely identifying principal
     * */
    public Principal getPrincipal();

    /**
     * Authentications from a token
     * throws UnsupportedAuthenticationMethod
     * throws InvalidAuthenticationToken
     * throws SubjectNotFound
     * throws SubjectDisabled
     */
    public void login(final AuthenticationToken token);


    /**
     * Updates/Adds attributes of/to this subject
     */
    public void updateSubjectAttributes(Map<String, String> attributes);

    /**
     * Remove given attributes of this subject
     */
    public void removeSubjectAttributes(List<String> attributesToBeRemoved);

}
