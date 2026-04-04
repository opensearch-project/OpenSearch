/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.List;

/**
 * DataFusion fragment converter. Converts stripped Calcite RelNode fragments
 * to Substrait plan bytes that the DataFusion Rust runtime can consume.
 *
 * <p>TODO: implement convertShuffleReadFragment, convertInMemoryFragment, appendShuffleWriter
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private static final SimpleExtension.ExtensionCollection EXTENSIONS =
        DefaultExtensionCatalog.DEFAULT_COLLECTION;

    @Override
    public byte[] convertScanFragment(String tableName, RelNode fragment) {
        LOGGER.info("Converting scan fragment for table [{}]:\n{}", tableName, RelOptUtil.toString(fragment));

        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream()
            .map(field -> field.getValue())
            .toList();

        Plan.Root substraitRoot = Plan.Root.builder()
            .input(substraitRel)
            .names(fieldNames)
            .build();

        Plan plan = Plan.builder().addRoots(substraitRoot).build();
        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);

        byte[] bytes = protoPlan.toByteArray();
        LOGGER.info("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = new TypeConverter(null);

        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            EXTENSIONS.aggregateFunctions(), typeFactory
        );
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            EXTENSIONS.scalarFunctions(), List.of(), typeFactory, typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            EXTENSIONS.windowFunctions(), typeFactory
        );

        return new SubstraitRelVisitor(
            typeFactory, scalarConverter, aggConverter, windowConverter,
            typeConverter, ImmutableFeatureBoard.builder().build()
        );
    }
}
