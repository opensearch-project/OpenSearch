/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.calcite;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a Calcite {@link RelNode} to Substrait protobuf bytes using Isthmus.
 *
 * <p>Supports custom function mappings via {@link FunctionMappings.Sig} so that
 * UDFs (e.g., CIDRMATCH) are serialized as Substrait extension functions.
 */
public class SubstraitConverter {

    private final SimpleExtension.ExtensionCollection extensions;
    private final List<FunctionMappings.Sig> customSigs;

    /**
     * Create a converter with default Substrait extensions and no custom functions.
     */
    public SubstraitConverter() {
        this(List.of());
    }

    /**
     * Create a converter with custom function mappings merged into the default extensions.
     *
     * @param customSigs     custom Calcite operator → Substrait function name mappings
     */
    public SubstraitConverter(List<FunctionMappings.Sig> customSigs) {
        this(customSigs, List.of());
    }

    /**
     * Create a converter with custom function mappings and custom extension YAML files.
     *
     * @param customSigs           custom Calcite operator → Substrait function name mappings
     * @param extensionYamlPaths   classpath paths to custom Substrait extension YAML files
     */
    public SubstraitConverter(List<FunctionMappings.Sig> customSigs, List<String> extensionYamlPaths) {
        this.customSigs = customSigs;
        SimpleExtension.ExtensionCollection base = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        if (!extensionYamlPaths.isEmpty()) {
            try {
                SimpleExtension.ExtensionCollection custom = SimpleExtension.load(extensionYamlPaths);
                this.extensions = base.merge(custom);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load custom Substrait extensions", e);
            }
        } else {
            this.extensions = base;
        }
    }

    /**
     * Convert a Calcite RelNode to Substrait protobuf bytes.
     */
    public byte[] convert(RelNode relNode) {
        RelRoot relRoot = RelRoot.of(relNode, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(relNode.getCluster().getTypeFactory());
        Rel substraitRel = visitor.apply(relRoot.rel);

        List<String> fieldNames = relRoot.fields.stream()
            .map(field -> field.getValue())
            .collect(Collectors.toList());

        Plan.Root substraitRoot = Plan.Root.builder()
            .input(substraitRel)
            .names(fieldNames)
            .build();

        Plan plan = Plan.builder()
            .addRoots(substraitRoot)
            .build();

        PlanProtoConverter planToProto = new PlanProtoConverter();
        io.substrait.proto.Plan protoPlan = planToProto.toProto(plan);
        return protoPlan.toByteArray();
    }

    private SubstraitRelVisitor createVisitor(RelDataTypeFactory typeFactory) {
        TypeConverter typeConverter = new TypeConverter(new io.substrait.isthmus.UserTypeMapper() {
            @Override
            public io.substrait.type.Type toSubstrait(org.apache.calcite.rel.type.RelDataType type) {
                return null;
            }

            @Override
            public org.apache.calcite.rel.type.RelDataType toCalcite(io.substrait.type.Type.UserDefined type) {
                return null;
            }
        });

        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            extensions.aggregateFunctions(), typeFactory);
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(), customSigs, typeFactory, typeConverter);
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            extensions.windowFunctions(), typeFactory);

        return new SubstraitRelVisitor(
            typeFactory, scalarConverter, aggConverter, windowConverter,
            typeConverter, ImmutableFeatureBoard.builder().build());
    }
}
