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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.pattern.ExtendedThrowablePatternConverter;
import org.opensearch.common.Strings;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Formats log events as strings in a json format.
 * <p>
 * The class is providing a JSON log format using jackson-core only (in contrast
 * to {@link org.apache.logging.log4j.core.layout.JsonLayout}).
 * There are default fields in the log line:
 * <ul>
 * <li>type - the type of logs. These represent appenders and help docker distinguish log streams.</li>
 * <li>timestamp - ISO8601 with additional timezone ID</li>
 * <li>level - INFO, WARN etc</li>
 * <li>component - logger name, most of the times class name</li>
 * <li>cluster.name - taken from sys:opensearch.logs.cluster_name system property because it is always set</li>
 * <li>node.name - taken from NodeNamePatternConverter, as it can be set in runtime as hostname when not set in opensearch.yml</li>
 * <li>message - a json escaped message. Multiline messages will be converted to single line with new line explicitly
 * <li>message - a json escaped message. Multiline messages will be converted to single line with new line explicitly
 * replaced to \n</li>
 * <li>cluster.uuid - as soon as populated
 * <li>node.id - as soon as populated
 * <li>stracktrace - in json as a stacktrace field. Only present when throwable is passed as a parameter when using a logger
 * and includeStacktrace is set to true. Field is a string[] unless stacktraceAsString is set to true.
 * </li>
 * </ul>
 * <p>
 * It is possible to add more opensearch specific fields with <code>opensearchmessagefield</code>
 * <code>
 * appender.logger.layout.opensearchmessagefields=message,took,took_millis,total_hits,types,stats,search_type,total_shards,source,id
 * </code>
 * Each of these will be expanded into a json field with a value taken {@link OpenSearchLogMessage} field. In the example above
 * <code>... "message":  %OpenSearchMessageField{message}, "took": %OpenSearchMessageField{took} ...</code>
 * the message passed to a logger will be overriden with a value from %OpenSearchMessageField{message}
 * <p>
 * The value taken from %OpenSearchMessageField{message} has to be a simple escaped JSON value and is populated in subclasses of
 * <code>OpenSearchLogMessage</code>
 * Using the additionalFields configuration, additional log4j2 patternlayout fields can be added, removed
 * or overridden
 * <code>
 * appender.logger.layout.additionalFields=threadid=%tid,node.name=,nodename=%node_name
 * </code>
 * <p>
 * Additional settings
 * <ul>
 * <li>appender.logger.layout.complete = *false*|true - write semantically complete json ([event,event,event,...]) </li>
 * <li>appender.logger.layout.compact = *false*|true - Use a compact json format (not intended, no newlines) </li>
 * <li>appender.logger.layout.eventEol = *false*|true - Write a line break in the end of the event, even in compact format </li>
 * <li>appender.logger.layout.endOfLine = "\r\n" - Newline pattern</li>
 * <li>appender.logger.layout.headerPattern = "[\r\n" - Header pattern in complete format</li>
 * <li>appender.logger.layout.footerPattern = "]\r\n" - Footer pattern in complete format</li>
 * <li>appender.logger.layout.stacktraceAsString = *false*|true - Stacktrace as single string</li>
 * </ul>
 *
 * @opensearch.internal
 */
@Plugin(name = "OpenSearchJsonLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true)
public class OpenSearchJsonLayout extends AbstractStringLayout {
    private static final String DEFAULT_FOOTER = "]";
    private static final String DEFAULT_HEADER = "[";
    protected static final String DEFAULT_EOL = "\r\n";
    protected static final String COMPACT_EOL = "";

    private final JsonFactory factory;
    private final String typeName;
    private final List<FieldEntry> fields;
    private final ExtendedThrowablePatternConverter throwablePatternConverter;
    private final boolean complete;
    private final boolean compact;
    private final String eol;
    private final boolean stacktraceAsString;
    private final String headerPattern;
    private final String footerPattern;

    private static class FieldEntry {
        private final String fieldName;
        private final boolean optional;
        private String pattern;
        private PatternLayout layout;

        public FieldEntry(String fieldName, boolean optional, String pattern) {
            this.fieldName = fieldName;
            this.optional = optional;
            setPattern(pattern);
        }

        public String getFieldName() {
            return fieldName;
        }

        public boolean isOptional() {
            return optional;
        }

        public void setPattern(String pattern) {
            if (Strings.isNullOrEmpty(pattern)) {
                pattern = null;
            }
            this.layout = createPatternLayout(pattern);
            this.pattern = pattern;
        }

        public String getPattern() {
            return this.pattern;
        }

        public String toSerializable(LogEvent event) {
            if (this.layout != null) {
                return this.layout.toSerializable(event);
            }
            return null;
        }
    }

    protected OpenSearchJsonLayout(
        String typeName,
        Charset charset,
        Map<String, String> additionalFields,
        boolean complete,
        boolean compact,
        boolean eventEol,
        String endOfLine,
        String headerPattern,
        String footerPattern,
        boolean stacktraceAsString
    ) {
        super(charset);

        this.typeName = typeName;
        this.complete = complete;
        this.compact = compact;
        this.eol = endOfLine != null ? endOfLine : compact && !eventEol ? COMPACT_EOL : DEFAULT_EOL;
        this.stacktraceAsString = stacktraceAsString;
        this.headerPattern = headerPattern == null ? (DEFAULT_HEADER + eol) : headerPattern;
        this.footerPattern = footerPattern == null ? (DEFAULT_FOOTER + eol) : footerPattern;
        this.throwablePatternConverter = ExtendedThrowablePatternConverter.newInstance(null, new String[0]);

        this.factory = new JsonFactory();
        this.fields = new ArrayList<>();
        fields.add(new FieldEntry("timestamp", false, "%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}"));
        fields.add(new FieldEntry("level", false, "%p"));
        fields.add(new FieldEntry("component", false, "%c{1.}"));
        fields.add(new FieldEntry("cluster.name", false, "${sys:opensearch.logs.cluster_name}"));
        fields.add(new FieldEntry("node.name", false, "%node_name"));
        fields.add(new FieldEntry("message", false, "%notEmpty{%marker{JSON} }%m{%.-10000m}{JSON}"));
        fields.add(new FieldEntry("cluster.uuid", true, "%node_and_cluster_id{cluster_uuid}"));
        fields.add(new FieldEntry("node.id", true, "%node_and_cluster_id{node_id}"));
        if (additionalFields != null) {
            for (Map.Entry<String, String> additionalField : additionalFields.entrySet()) {
                FieldEntry entry = fields.stream().filter(f -> additionalField.getKey().equals(f.fieldName)).findFirst().orElse(null);
                if (entry == null) {
                    entry = new FieldEntry(additionalField.getKey(), true, additionalField.getValue());
                    fields.add(entry);
                } else {
                    entry.setPattern(additionalField.getValue());
                }
            }
        }
    }

    private static PatternLayout createPatternLayout(String pattern) {
        if (Strings.isNullOrEmpty(pattern)) return null;

        return PatternLayout.newBuilder().withPattern(pattern).withAlwaysWriteExceptions(false).build();
    }

    @Override
    public String toSerializable(final LogEvent event) {
        try {
            StringWriter jsonObjectWriter = new StringWriter();
            if (this.complete && this.eventCount > 0) {
                jsonObjectWriter.write(",");
            }
            JsonGenerator generator = factory.createGenerator(jsonObjectWriter);
            if (!this.compact) generator.useDefaultPrettyPrinter();
            generator.writeStartObject();
            generator.writeStringField("type", this.typeName);
            for (FieldEntry field : this.fields) {
                String value = field.toSerializable(event);
                if (!field.isOptional() || !Strings.isNullOrEmpty(value)) {
                    generator.writeStringField(field.getFieldName(), value);
                }
            }
            if (event.getThrown() != null || event.getThrownProxy() != null) {
                StringBuilder sb = new StringBuilder();
                throwablePatternConverter.format(event, sb);
                if (this.stacktraceAsString) {
                    generator.writeStringField("stacktrace", sb.toString());
                } else {
                    List<String> lines = Arrays.stream(sb.toString().split(System.lineSeparator())).collect(Collectors.toList());
                    generator.writeFieldName("stacktrace");
                    generator.writeStartArray();
                    for (String line : lines) {
                        generator.writeString(line);
                    }
                    generator.writeEndArray();
                }
            }

            generator.close();
            jsonObjectWriter.write(this.eol);
            markEvent();
            return jsonObjectWriter.toString();
        } catch (IOException e) {
            // Should this be an ISE or IAE?
            // LOGGER.error(e);
            return "log error: " + e.getMessage();
        }
    }

    /**
     * Returns appropriate JSON header.
     *
     * @return a byte array containing the header, opening the JSON array.
     */
    @Override
    public byte[] getHeader() {
        if (!this.complete) {
            return null;
        }
        return getBytes(headerPattern);
    }

    /**
     * Returns appropriate JSON footer.
     *
     * @return a byte array containing the footer, closing the JSON array.
     */
    @Override
    public byte[] getFooter() {
        if (!this.complete) {
            return null;
        }
        return getBytes(footerPattern);
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("OpenSearchJsonLayout{");
        sb.append("type=").append(this.typeName).append(",");
        sb.append("complete=").append(this.complete).append(",");
        sb.append("compact=").append(this.compact).append(",");
        sb.append("eol=").append(JsonStringEncoder.getInstance().quoteAsString(this.eol)).append(",");
        sb.append("stacktraceAsString=").append(this.stacktraceAsString).append(",");
        sb.append("headerPattern=").append(JsonStringEncoder.getInstance().quoteAsString(this.headerPattern)).append(",");
        sb.append("footerPattern=").append(JsonStringEncoder.getInstance().quoteAsString(this.footerPattern)).append(",");
        sb.append("Fields{");
        for (FieldEntry field : fields) {
            if (field.getPattern() != null) {
                sb.append(field.getFieldName()).append("=").append(field.getPattern()).append(",");
            }
        }
        sb.append("}}");
        return sb.toString();
    }

    @PluginFactory
    public static OpenSearchJsonLayout createLayout(
        String type,
        Charset charset,
        Map<String, String> additionalFields,
        boolean complete,
        boolean compact,
        boolean eventEol,
        String endOfLine,
        String headerPattern,
        String footerPattern,
        boolean stacktraceAsString
    ) {
        return new OpenSearchJsonLayout(
            type,
            charset,
            additionalFields,
            complete,
            compact,
            eventEol,
            endOfLine,
            headerPattern,
            footerPattern,
            stacktraceAsString
        );
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

        @PluginAttribute("additionalFields")
        private String additionalFields;

        @PluginAttribute("complete")
        private boolean complete;

        @PluginAttribute("compact")
        private boolean compact;

        @PluginAttribute("eventEol")
        private boolean eventEol;

        @PluginAttribute("endOfLine")
        private String endOfLine;

        @PluginAttribute("headerPattern")
        private String headerPattern;

        @PluginAttribute("footerPattern")
        private String footerPattern;

        @PluginAttribute("stacktraceAsString")
        private boolean stacktraceAsString;

        public Builder() {
            setCharset(StandardCharsets.UTF_8);
        }

        @Override
        public OpenSearchJsonLayout build() {
            HashMap<String, String> parsedFields = new HashMap<>();
            if (!Strings.isNullOrEmpty(additionalFields)) {
                String[] fields = additionalFields.split(",");
                for (String field : fields) {
                    String[] split = field.split("=", 2);
                    if (split.length == 2) {
                        parsedFields.put(split[0], split[1]);
                    }
                }
            }

            // Old behavior
            String[] messageFields = Strings.isNullOrEmpty(opensearchMessageFields) ? new String[] {} : opensearchMessageFields.split(",");
            for (String messageField : messageFields) {
                if (!parsedFields.containsKey(messageField)) {
                    parsedFields.put(messageField, "%OpenSearchMessageField{" + messageField + "}");
                }
            }
            return OpenSearchJsonLayout.createLayout(
                type,
                charset,
                parsedFields,
                complete,
                compact,
                eventEol,
                endOfLine,
                headerPattern,
                footerPattern,
                stacktraceAsString
            );
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

        public boolean isComplete() {
            return complete;
        }

        public B setComplete(boolean complete) {
            this.complete = complete;
            return asBuilder();
        }

        public boolean isCompact() {
            return compact;
        }

        public B setCompact(boolean compact) {
            this.compact = compact;
            return asBuilder();
        }

        public boolean isEventEol() {
            return eventEol;
        }

        public B setEventEol(boolean eventEol) {
            this.eventEol = eventEol;
            return asBuilder();
        }

        public String getEndOfLine() {
            return endOfLine;
        }

        public B setEndOfLine(String endOfLine) {
            this.endOfLine = endOfLine;
            return asBuilder();
        }

        public String getHeaderPattern() {
            return headerPattern;
        }

        public B setHeaderPattern(String headerPattern) {
            this.headerPattern = headerPattern;
            return asBuilder();
        }

        public String getFooterPattern() {
            return footerPattern;
        }

        public B setFooterPattern(String footerPattern) {
            this.footerPattern = footerPattern;
            return asBuilder();
        }

        public boolean isStacktraceAsString() {
            return stacktraceAsString;
        }

        public B setStacktraceAsString(boolean stacktraceAsString) {
            this.stacktraceAsString = stacktraceAsString;
            return asBuilder();
        }

        public String getOpenSearchMessageFields() {
            return opensearchMessageFields;
        }

        public B setOpenSearchMessageFields(String opensearchMessageFields) {
            this.opensearchMessageFields = opensearchMessageFields;
            return asBuilder();
        }

        public String getAdditionalFields() {
            return additionalFields;
        }

        public B setAdditionalFields(String additionalFields) {
            this.additionalFields = additionalFields;
            return asBuilder();
        }
    }

    @PluginBuilderFactory
    public static <B extends OpenSearchJsonLayout.Builder<B>> B newBuilder() {
        return new OpenSearchJsonLayout.Builder<B>().asBuilder();
    }
}
