/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.MakeStructFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;

/**
 * Serializes {@code make_struct} to a Substrait {@link Expression.ScalarFunctionInvocation} built
 * directly, rather than letting isthmus match it against a declared signature. Operands are
 * forwarded unchanged, including the field-name literals, since DataFusion's {@code named_struct}
 * takes the interleaved {@code (name, value, …)} form.
 *
 * <p>Bypassing the matcher is what leaves the number of struct fields unbounded: isthmus binds a
 * variadic function through a {@code SingularArgumentMatcher}, which derives one type every operand
 * must satisfy, and {@code named_struct}'s interleaved operands have no such common type. Declaring
 * one impl per field count matches but caps struct width. The extension declaration is therefore
 * used only as an anchor (name + URN) for the consumer to resolve by name.
 *
 * <p>Substrait's native {@code Expression.NestedStruct} would be cleaner, but DataFusion rejects it
 * at execution ("Nested struct expressions are not yet supported"). Revisit if that closes.
 *
 * @opensearch.internal
 */
class MakeStructCallConverter implements CallConverter {

    /** DataFusion's native struct constructor — the name the consumer resolves. */
    static final String NAMED_STRUCT = "named_struct";

    private final SimpleExtension.ExtensionCollection extensions;
    private final TypeConverter typeConverter;

    MakeStructCallConverter(SimpleExtension.ExtensionCollection extensions, TypeConverter typeConverter) {
        this.extensions = extensions;
        this.typeConverter = typeConverter;
    }

    @Override
    public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter) {
        String operator = call.getOperator().getName();
        // The engine emits `make_struct`; `named_struct` is accepted too so the converter stays
        // correct if a rename ever reaches it first.
        if (!MakeStructFunction.NAME.equalsIgnoreCase(operator) && !NAMED_STRUCT.equalsIgnoreCase(operator)) {
            return Optional.empty();
        }

        Optional<SimpleExtension.ScalarFunctionVariant> declaration = findNamedStructDeclaration();
        if (declaration.isEmpty()) {
            // No anchor to reference — decline so the failure surfaces as isthmus' normal
            // "Unable to convert call" rather than an NPE deep in proto serialization.
            return Optional.empty();
        }

        List<FunctionArg> arguments = new ArrayList<>(call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            arguments.add(topLevelConverter.apply(operand));
        }

        return Optional.of(
            Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration.get())
                .addAllArguments(arguments)
                .outputType(typeConverter.toSubstrait(call.getType()))
                .build()
        );
    }

    /**
     * Any declared {@code named_struct} variant works as the anchor — the consumer resolves the
     * function by name, and the arity we attach is independent of the variant's declared arity.
     */
    private Optional<SimpleExtension.ScalarFunctionVariant> findNamedStructDeclaration() {
        return extensions.scalarFunctions().stream().filter(variant -> NAMED_STRUCT.equals(variant.name())).findFirst();
    }
}
