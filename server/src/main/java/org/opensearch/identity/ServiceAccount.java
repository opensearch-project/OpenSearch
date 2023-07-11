/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.Objects;
import org.opensearch.Application;

/**
 * This interface defines a ServiceAccount.
 *
 * A ServiceAccount is a principal which can retrieve its associated permissions.
 */
public class ServiceAccount implements Principal {

    private final Application application;
    private final Principal name;

    /**
     * Creates a principal for an application identity
     * @param application The application to be associated with the service account
     */
    public ServiceAccount(final Application application) {

        this.application = application;
        name = application.getPrincipal();
    }

    @Override
    public String getName() {
        return name.getName();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final ServiceAccount that = (ServiceAccount) obj;
        return Objects.equals(name, that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "ServiceAccount(" + "name=" + name + ")";
    }
}
