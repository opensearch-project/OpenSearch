/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.identity;

import java.util.List;

/**
 * An authenticated user in the system.
 */
public class UserIdentity implements Identity {

    /** No identity information or identity information is unavaliable */
    public static final AnonymousUser ANONYMOUS = new AnonymousUser();

    /** A persistant identifier that is unqiue to the user */
    private String id;

    public UserIdentity(final String id) {
        this.id = id;
    }

    /** A durable and reusable identifier of this user */
    public String getId() {
        return this.id;
    }

    /** No identity information or identity information is unavaliable */
    public static final class AnonymousUser extends UserIdentity {
        static final String ID = "Anonymous";
        protected AnonymousUser() {
            super(AnonymousUser.ID);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AnonymousUser;
        }

        @Override
        public int hashCode() {
            return getId().hashCode();
        }
    }
}
