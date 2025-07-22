/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.logging;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.Message;

/**
 * Appends warnings to the header
 *
 * @opensearch.internal
 */
@Plugin(name = "HeaderWarningAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class HeaderWarningAppender extends AbstractAppender {
    public HeaderWarningAppender(String name, Filter filter) {
        super(name, filter, null);
    }

    @Override
    public void append(LogEvent event) {
        final Message message = event.getMessage();

        if (message instanceof OpenSearchLogMessage) {
            final OpenSearchLogMessage opensearchLogMessage = (OpenSearchLogMessage) message;

            String messagePattern = opensearchLogMessage.getMessagePattern();
            Object[] arguments = opensearchLogMessage.getArguments();

            HeaderWarning.addWarning(messagePattern, arguments);
        } else {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            HeaderWarning.addWarning(formattedMessage);
        }
    }

    @PluginFactory
    public static HeaderWarningAppender createAppender(@PluginAttribute("name") String name, @PluginElement("filter") Filter filter) {
        return new HeaderWarningAppender(name, filter);
    }
}
