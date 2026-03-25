/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Converts {@code _source} field selection to a {@link LogicalProject}.
 * Handles exact field names, wildcard patterns, and {@code _source: false}.
 */
public class ProjectConverter extends AbstractDslConverter {

    /** Creates a project converter. */
    public ProjectConverter() {}

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getSearchSource().fetchSource() != null;
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        FetchSourceContext fetchSource = ctx.getSearchSource().fetchSource();

        if (!fetchSource.fetchSource()) {
            return LogicalProject.create(input, List.of(), List.of(), List.of());
        }

        String[] includes = fetchSource.includes();
        String[] excludes = fetchSource.excludes();
        boolean hasIncludes = includes != null && includes.length > 0;
        boolean hasExcludes = excludes != null && excludes.length > 0;

        if (!hasIncludes && !hasExcludes) {
            return input;
        }

        return createProjection(input, includes, excludes, ctx.getRexBuilder());
    }

    private RelNode createProjection(RelNode input, String[] includes, String[] excludes, RexBuilder rexBuilder)
            throws ConversionException {
        RelDataType rowType = input.getRowType();
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        if (includes != null && includes.length > 0) {
            // Include mode: only listed fields
            for (String pattern : includes) {
                if (pattern.contains("*")) {
                    resolveWildcard(pattern, rowType, rexBuilder, projects, fieldNames);
                } else {
                    resolveField(pattern, rowType, rexBuilder, projects, fieldNames);
                }
            }
        } else {
            // Exclude-only mode: all fields except excluded
            Set<String> excludeSet = buildExcludeSet(excludes, rowType);
            for (RelDataTypeField field : rowType.getFieldList()) {
                if (!excludeSet.contains(field.getName())) {
                    projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
                    fieldNames.add(field.getName());
                }
            }
        }

        return LogicalProject.create(input, List.of(), projects, fieldNames);
    }

    private Set<String> buildExcludeSet(String[] excludes, RelDataType rowType) {
        Set<String> excludeSet = new HashSet<>();
        for (String pattern : excludes) {
            if (pattern.contains("*")) {
                Pattern compiled = Pattern.compile(pattern.replace(".", "\\.").replace("*", ".*"));
                for (RelDataTypeField field : rowType.getFieldList()) {
                    if (compiled.matcher(field.getName()).matches()) {
                        excludeSet.add(field.getName());
                    }
                }
            } else {
                excludeSet.add(pattern);
            }
        }
        return excludeSet;
    }

    private void resolveWildcard(String pattern, RelDataType rowType, RexBuilder rexBuilder,
            List<RexNode> projects, List<String> fieldNames) {
        Pattern compiled = Pattern.compile(pattern.replace(".", "\\.").replace("*", ".*"));
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (compiled.matcher(field.getName()).matches()) {
                projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
                fieldNames.add(field.getName());
            }
        }
        // No error if nothing matches — consistent with OpenSearch core, which returns empty _source
    }

    private void resolveField(String fieldName, RelDataType rowType, RexBuilder rexBuilder,
            List<RexNode> projects, List<String> fieldNames) throws ConversionException {
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }
        projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
        fieldNames.add(field.getName());
    }
}
