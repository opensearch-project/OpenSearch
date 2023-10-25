/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.inject;

import org.opensearch.common.inject.internal.Errors;
import org.opensearch.common.inject.spi.Message;

/**
 * Handles {@link Binder#addError} commands.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
final class MessageProcessor extends AbstractProcessor {

    // private static final Logger logger = Logger.getLogger(Guice.class.getName());

    MessageProcessor(final Errors errors) {
        super(errors);
    }

    @Override
    public Boolean visit(final Message message) {
        // OPENSEARCH_GUICE: don't log failures using jdk logging
        // if (message.getCause() != null) {
        // String rootMessage = getRootMessage(message.getCause());
        // logger.log(Level.INFO,
        // "An exception was caught and reported. Message: " + rootMessage,
        // message.getCause());
        // }

        errors.addMessage(message);
        return true;
    }
}
