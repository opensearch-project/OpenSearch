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

import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.opensearch.core.common.Strings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Formats log events as strings in a json format.
 * <p>
 * The class is wrapping the {@link PatternLayout} with a pattern to format into json. This gives more flexibility and control over how the
 * log messages are formatted in {@link org.apache.logging.log4j.core.layout.JsonLayout}
 * There are fields which are always present in the log line:
 * <ul>
 * <li>type - the type of logs. These represent appenders and help docker distinguish log streams.</li>
 * <li>timestamp - ISO8601 with additional timezone ID</li>
 * <li>level - INFO, WARN etc</li>
 * <li>component - logger name, most of the times class name</li>
 * <li>cluster.name - taken from sys:opensearch.logs.cluster_name system property because it is always set</li>
 * <li>node.name - taken from NodeNamePatternConverter, as it can be set in runtime as hostname when not set in opensearch.yml</li>
 * <li>node_and_cluster_id - in json as node.id and cluster.uuid - taken from NodeAndClusterIdConverter and present
 * once clusterStateUpdate is first received</li>
 * <li>message - a json escaped message. Multiline messages will be converted to single line with new line explicitly
 * replaced to \n</li>
 * <li>exceptionAsJson - in json as a stacktrace field. Only present when throwable is passed as a parameter when using a logger.
 * Taken from JsonThrowablePatternConverter</li>
 * </ul>
 * <p>
 * It is possible to add more or override them with <code>opensearchmessagefield</code>
 * <code>
 * appender.logger.layout.opensearchmessagefields=message,took,took_millis,total_hits,types,stats,search_type,total_shards,source,id
 * </code>
 * Each of these will be expanded into a json field with a value taken {@link OpenSearchLogMessage} field. In the example above
 * <code>... "message":  %OpenSearchMessageField{message}, "took": %OpenSearchMessageField{took} ...</code>
 * the message passed to a logger will be overriden with a value from %OpenSearchMessageField{message}
 * <p>
 * The value taken from %OpenSearchMessageField{message} has to be a simple escaped JSON value and is populated in subclasses of
 * <code>OpenSearchLogMessage</code>
 * <p>
 * The <code>message</code> field is truncated at 10000 characters by default. This limit can be controlled by setting the
 * <code>appender.logger.layout.maxmessagelength</code> to the desired limit or to <code>0</code> to disable truncation.
 *
 * @opensearch.internal
 */
@Plugin(name = "OpenSearchJsonLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class OpenSearchJsonLayout extends AbstractStringLayout {

    private final PatternLayout patternLayout;

    protected OpenSearchJsonLayout(
        String typeName,
        Charset charset,
        String[] opensearchMessageFields,
        int maxMessageLength,
        Configuration configuration
    ) {
        super(charset);
        this.patternLayout = PatternLayout.newBuilder()
            .withPattern(pattern(typeName, opensearchMessageFields, maxMessageLength))
            .withAlwaysWriteExceptions(false)
            .withConfiguration(configuration)
            .build();
    }

    private String pattern(String type, String[] opensearchMessageFields, int maxMessageLength) {
        if (Strings.isEmpty(type)) {
            throw new IllegalArgumentException("layout parameter 'type_name' cannot be empty");
        }

        String messageFormat = "%m";
        if (maxMessageLength < 0) {
            throw new IllegalArgumentException("layout parameter 'maxmessagelength' cannot be a negative number");
        } else if (maxMessageLength > 0) {
            messageFormat = "%.-" + Integer.toString(maxMessageLength) + "m";
        }

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", inQuotes(type));
        map.put("timestamp", inQuotes("%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}"));
        map.put("level", inQuotes("%p"));
        map.put("component", inQuotes("%c{1.}"));
        map.put("cluster.name", inQuotes("${sys:opensearch.logs.cluster_name}"));
        map.put("node.name", inQuotes("%node_name"));
        map.put("message", inQuotes("%notEmpty{%enc{%marker}{JSON} }%enc{" + messageFormat + "}{JSON}"));

        for (String key : opensearchMessageFields) {
            map.put(key, inQuotes("%OpenSearchMessageField{" + key + "}"));
        }
        return createPattern(map, Stream.of(opensearchMessageFields).collect(Collectors.toSet()));
    }

    private String createPattern(Map<String, Object> map, Set<String> opensearchMessageFields) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        String separator = "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {

            if (opensearchMessageFields.contains(entry.getKey())) {
                sb.append("%notEmpty{");
                sb.append(separator);
                appendField(sb, entry);
                sb.append("}");
            } else {
                sb.append(separator);
                appendField(sb, entry);
            }

            separator = ", ";
        }
        sb.append(notEmpty(", %node_and_cluster_id "));
        sb.append("%exceptionAsJson ");
        sb.append("}");
        sb.append(System.lineSeparator());

        return sb.toString();
    }

    private void appendField(StringBuilder sb, Map.Entry<String, Object> entry) {
        sb.append(jsonKey(entry.getKey()));
        sb.append(entry.getValue().toString());
    }

    private String notEmpty(String value) {
        return "%notEmpty{" + value + "}";
    }

    private CharSequence jsonKey(String s) {
        return inQuotes(s) + ": ";
    }

    private String inQuotes(String s) {
        return "\"" + s + "\"";
    }

    @PluginFactory
    public static OpenSearchJsonLayout createLayout(
        String type,
        Charset charset,
        String[] opensearchmessagefields,
        int maxMessageLength,
        Configuration configuration
    ) {
        return new OpenSearchJsonLayout(type, charset, opensearchmessagefields, maxMessageLength, configuration);
    }

    PatternLayout getPatternLayout() {
        return patternLayout;
    }

    /**
     * Builder for a json layout
     *
     * @opensearch.internal
     */
    public static class Builder<B extends OpenSearchJsonLayout.Builder<B>> extends AbstractStringLayout.Builder<B>
        implements
            org.apache.logging.log4j.core.util.Builder<OpenSearchJsonLayout> {

        @PluginAttribute("type_name")
        String type;

        @PluginAttribute(value = "charset", defaultString = "UTF-8")
        Charset charset;

        @PluginAttribute("opensearchmessagefields")
        private String opensearchMessageFields;

        @PluginAttribute(value = "maxmessagelength", defaultInt = 10000)
        private int maxMessageLength;

        @PluginConfiguration
        private Configuration configuration;

        public Builder() {
            setCharset(StandardCharsets.UTF_8);
            setMaxMessageLength(10000);
        }

        @Override
        public OpenSearchJsonLayout build() {
            String[] split = Strings.isNullOrEmpty(opensearchMessageFields) ? new String[] {} : opensearchMessageFields.split(",");
            return OpenSearchJsonLayout.createLayout(type, charset, split, maxMessageLength, configuration);
        }

        public Charset getCharset() {
            return charset;
        }

        public B setCharset(final Charset charset) {
            this.charset = charset;
            return asBuilder();
        }

        public String getType() {
            return type;
        }

        public B setType(final String type) {
            this.type = type;
            return asBuilder();
        }

        public String getOpenSearchMessageFields() {
            return opensearchMessageFields;
        }

        public B setOpenSearchMessageFields(String opensearchMessageFields) {
            this.opensearchMessageFields = opensearchMessageFields;
            return asBuilder();
        }

        public int getMaxMessageLength() {
            return maxMessageLength;
        }

        public B setMaxMessageLength(final int maxMessageLength) {
            this.maxMessageLength = maxMessageLength;
            return asBuilder();
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public B setConfiguration(final Configuration configuration) {
            this.configuration = configuration;
            return asBuilder();
        }
    }

    @PluginBuilderFactory
    public static <B extends OpenSearchJsonLayout.Builder<B>> B newBuilder() {
        return new OpenSearchJsonLayout.Builder<B>().asBuilder();
    }

    @Override
    public String toSerializable(final LogEvent event) {
        return patternLayout.toSerializable(event);
    }

    @Override
    public Map<String, String> getContentFormat() {
        return patternLayout.getContentFormat();
    }

    @Override
    public void encode(final LogEvent event, final ByteBufferDestination destination) {
        patternLayout.encode(event, destination);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpenSearchJsonLayout{");
        sb.append("patternLayout=").append(patternLayout);
        sb.append('}');
        return sb.toString();
    }
}
