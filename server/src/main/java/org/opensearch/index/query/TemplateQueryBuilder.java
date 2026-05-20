/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.Query;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A query builder that constructs a query based on a template and context variables.
 * This query is designed to be rewritten with variables from search processors.
 */

public class TemplateQueryBuilder extends AbstractQueryBuilder<TemplateQueryBuilder> {
    public static final String NAME = "template";
    public static final String queryName = "template";
    private final Map<String, Object> content;

    /**
     * Constructs a new TemplateQueryBuilder with the given content.
     *
     * @param content The template content as a map.
     */
    public TemplateQueryBuilder(Map<String, Object> content) {
        this.content = content;
    }

    /**
     * Creates a TemplateQueryBuilder from XContent.
     *
     * @param parser The XContentParser to read from.
     * @return A new TemplateQueryBuilder instance.
     * @throws IOException If there's an error parsing the content.
     */
    public static TemplateQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return new TemplateQueryBuilder(parser.map());
    }

    /**
     * Constructs a TemplateQueryBuilder from a stream input.
     *
     * @param in The StreamInput to read from.
     * @throws IOException If there's an error reading from the stream.
     */
    public TemplateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.content = in.readMap();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(content);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, content);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new IllegalStateException(
            "Template queries cannot be converted directly to a query. Template Query must be rewritten first during doRewrite."
        );
    }

    @Override
    protected boolean doEquals(TemplateQueryBuilder other) {
        return Objects.equals(this.content, other.content);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(content);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Gets the content of this template query.
     *
     * @return The template content as a map.
     */
    public Map<String, Object> getContent() {
        return content;
    }

    /**
     * Rewrites the template query by substituting variables from the context.
     *
     * @param queryCoordinatorContext The context for query rewriting.
     * @return A rewritten QueryBuilder.
     * @throws IOException If there's an error during rewriting.
     */
    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryCoordinatorContext) throws IOException {
        // the queryRewrite is expected at QueryCoordinator level
        if (!(queryCoordinatorContext instanceof QueryCoordinatorContext)) {
            throw new IllegalStateException(
                "Template Query must be rewritten at the coordinator node. Rewriting at shard level is not supported."
            );
        }

        QueryCoordinatorContext convertedQueryCoordinateContext = (QueryCoordinatorContext) queryCoordinatorContext;
        Map<String, Object> contextVariables = convertedQueryCoordinateContext.getContextVariables();
        String queryString;

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(this.content);
            queryString = builder.toString();
        }

        // Convert Map<String, Object> to Map<String, String> with proper JSON escaping
        Map<String, String> variablesMap = null;
        if (contextVariables != null) {
            variablesMap = contextVariables.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                try {
                    return JsonXContent.contentBuilder().value(entry.getValue()).toString();
                } catch (IOException e) {
                    throw new RuntimeException("Error converting contextVariables to JSON string", e);
                }
            }));
        }
        String newQueryContent = replaceVariables(queryString, variablesMap);

        try {
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(queryCoordinatorContext.getXContentRegistry(), LoggingDeprecationHandler.INSTANCE, newQueryContent);

            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

            QueryBuilder newQueryBuilder = parseInnerQueryBuilder(parser);

            return newQueryBuilder;

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to rewrite template query: " + newQueryContent, e);
        }
    }

    private String replaceVariables(String template, Map<String, String> variables) {
        if (template == null || template.equals("null")) {
            throw new IllegalArgumentException("Template string cannot be null. A valid template must be provided.");
        }
        if (template.isEmpty() || template.equals("{}")) {
            throw new IllegalArgumentException("Template string cannot be empty. A valid template must be provided.");
        }
        if (variables == null || variables.isEmpty()) {
            return template;
        }

        StringBuilder result = new StringBuilder();
        int start = 0;
        while (true) {
            int startVar = template.indexOf("\"${", start);
            if (startVar == -1) {
                result.append(template.substring(start));
                break;
            }
            result.append(template, start, startVar);
            int endVar = template.indexOf("}\"", startVar);
            if (endVar == -1) {
                throw new IllegalArgumentException("Unclosed variable in template: " + template.substring(startVar));
            }
            String varName = template.substring(startVar + 3, endVar);
            String replacement = variables.get(varName);
            if (replacement == null) {
                throw new IllegalArgumentException("Variable not found: " + varName);
            }
            result.append(replacement);
            start = endVar + 2;
        }
        return result.toString();
    }

}
