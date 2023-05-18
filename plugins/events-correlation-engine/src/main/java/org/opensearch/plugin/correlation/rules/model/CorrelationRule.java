/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.rules.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Correlation Rule DSL
 * {
 *   "name": "s3 to app logs",
 *   "correlate": [
 *     {
 *       "index": "s3_access_logs",
 *       "query": "aws.cloudtrail.eventName:ReplicateObject",
 *       "timestampField": "@timestamp",
 *       "tags": [
 *         "s3"
 *       ]
 *     }
 *   ]
 * }
 *
 * @opensearch.api
 * @opensearch.experimental
 */
public class CorrelationRule implements Writeable, ToXContentObject {

    private static final Logger log = LogManager.getLogger(CorrelationRule.class);

    /**
     * Correlation Rule Index
     */
    public static final String CORRELATION_RULE_INDEX = ".opensearch-correlation-rules-config";

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField CORRELATION_QUERIES_FIELD = new ParseField("correlate");
    private static final ObjectParser<CorrelationRule, Void> PARSER = new ObjectParser<>("CorrelationRule", CorrelationRule::new);

    static {
        PARSER.declareString(CorrelationRule::setId, ID_FIELD);
        PARSER.declareLong(CorrelationRule::setVersion, VERSION_FIELD);
        PARSER.declareString(CorrelationRule::setName, NAME_FIELD);
        PARSER.declareField((xcp, rule, context) -> {
            List<CorrelationQuery> correlationQueries = new ArrayList<>();
            XContentParser.Token currentToken = xcp.currentToken();
            if (currentToken == XContentParser.Token.START_ARRAY) {
                while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                    correlationQueries.add(CorrelationQuery.parse(xcp));
                }
            }
            rule.setCorrelationQueries(correlationQueries);
        }, CORRELATION_QUERIES_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    private String id;

    private Long version;

    private String name;

    private List<CorrelationQuery> correlationQueries;

    private CorrelationRule() {}

    /**
     * Parameterized ctor of Correlation Rule
     * @param name name of rule
     * @param correlationQueries list of correlation queries part of rule
     */
    public CorrelationRule(String name, List<CorrelationQuery> correlationQueries) {
        this("", 1L, name, correlationQueries);
    }

    /**
     * Parameterized ctor of Correlation Rule
     * @param id id of rule
     * @param version version of rule
     * @param name name of rule
     * @param correlationQueries list of correlation queries part of rule
     */
    public CorrelationRule(String id, Long version, String name, List<CorrelationQuery> correlationQueries) {
        this.id = id;
        this.version = version;
        this.name = name;
        this.correlationQueries = correlationQueries;
    }

    /**
     * StreamInput ctor of Correlation Rule
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public CorrelationRule(StreamInput sin) throws IOException {
        this(sin.readString(), sin.readLong(), sin.readString(), sin.readList(CorrelationQuery::readFrom));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.field(NAME_FIELD.getPreferredName(), name);

        CorrelationQuery[] correlationQueries = new CorrelationQuery[] {};
        correlationQueries = this.correlationQueries.toArray(correlationQueries);
        builder.field(CORRELATION_QUERIES_FIELD.getPreferredName(), correlationQueries);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeString(name);

        for (CorrelationQuery query : correlationQueries) {
            query.writeTo(out);
        }
    }

    /**
     * parse into CorrelationRule
     * @param xcp XContentParser
     * @param id id of rule
     * @param version version of rule
     * @return CorrelationRule
     */
    public static CorrelationRule parse(XContentParser xcp, String id, Long version) {
        return PARSER.apply(xcp, null);
    }

    /**
     * convert StreamInput to CorrelationRule
     * @param sin StreamInput
     * @return CorrelationRule
     * @throws IOException IOException
     */
    public static CorrelationRule readFrom(StreamInput sin) throws IOException {
        return new CorrelationRule(sin);
    }

    /**
     * set id
     * @param id id of rule
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * get id
     * @return id of rule
     */
    public String getId() {
        return id;
    }

    /**
     * set version
     * @param version version of rule
     */
    public void setVersion(Long version) {
        this.version = version;
    }

    /**
     * get version
     * @return version of rule
     */
    public Long getVersion() {
        return version;
    }

    /**
     * set name
     * @param name name of rule
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * get name
     * @return name of rule
     */
    public String getName() {
        return name;
    }

    /**
     * set correlation queries
     * @param correlationQueries set correlation queries for the rule
     */
    public void setCorrelationQueries(List<CorrelationQuery> correlationQueries) {
        this.correlationQueries = correlationQueries;
    }

    /**
     * get correlation queries
     * @return correlation queries for the rule
     */
    public List<CorrelationQuery> getCorrelationQueries() {
        return correlationQueries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CorrelationRule that = (CorrelationRule) o;
        return id.equals(that.id)
            && version.equals(that.version)
            && name.equals(that.name)
            && correlationQueries.equals(that.correlationQueries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, name, correlationQueries);
    }
}
