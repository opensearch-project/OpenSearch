/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.NameBasedAggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        LOGGER.debug("Converting shard scan fragment for table [{}]", tableName);
        return convertToSubstrait(fragment);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("Multi-stage partial aggregate not yet implemented");
    }

    @Override
    public byte[] convertFinalAggFragment(RelNode fragment) {
        throw new UnsupportedOperationException("Multi-stage final aggregate not yet implemented");
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("Multi-stage fragment attachment not yet implemented");
    }

    private byte[] convertToSubstrait(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = new TableNameModifier().modifyTableNames(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        // PPL `take` isn't in standard Calcite — emit a stub SqlAggFunction whose only
        // job is to seed the converter's name→FunctionFinder map so
        // NameBasedAggregateFunctionConverter can route the actual PPL TAKE operator
        // instance (a different Java object) by case-insensitive name match.
        List<FunctionMappings.Sig> additionalAggSigs = List.of(FunctionMappings.s(stubAgg("take"), "take"));
        AggregateFunctionConverter aggConverter = new NameBasedAggregateFunctionConverter(
            extensions.aggregateFunctions(),
            additionalAggSigs,
            typeFactory,
            typeConverter
        );
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(),
            List.of(),
            typeFactory,
            typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);

        return new SubstraitRelVisitor(
            typeFactory,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter,
            ImmutableFeatureBoard.builder().build()
        );
    }

    /**
     * Minimal {@link SqlAggFunction} acting as a name→FunctionFinder map key.
     * PPL emits its own SqlAggFunction instances; identity lookup against these
     * stubs misses, but {@link NameBasedAggregateFunctionConverter} falls back
     * to matching on operator name, which the stub provides.
     */
    private static SqlAggFunction stubAgg(String name) {
        return new SqlAggFunction(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        ) {};
    }

    /**
     * Strips Calcite catalog prefixes (e.g. "opensearch") from {@link NamedScan} table
     * names — DataFusion expects the bare index / stage-input id.
     */
    private static class TableNameModifier {
        Plan modifyTableNames(Plan plan) {
            TableNameVisitor visitor = new TableNameVisitor();
            List<Plan.Root> modifiedRoots = new ArrayList<>();
            for (Plan.Root root : plan.getRoots()) {
                Optional<Rel> modifiedRel = root.getInput().accept(visitor, null);
                if (modifiedRel.isPresent()) {
                    modifiedRoots.add(Plan.Root.builder().from(root).input(modifiedRel.get()).build());
                } else {
                    modifiedRoots.add(root);
                }
            }
            return Plan.builder().from(plan).roots(modifiedRoots).build();
        }

        private static class TableNameVisitor extends RelCopyOnWriteVisitor<RuntimeException> {
            @Override
            public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
                List<String> names = namedScan.getNames();
                if (names.size() > 1) {
                    return Optional.of(NamedScan.builder().from(namedScan).names(List.of(names.get(names.size() - 1))).build());
                }
                return super.visit(namedScan, context);
            }
        }
    }
}
