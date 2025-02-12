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

package org.opensearch.painless.phase;

import org.opensearch.painless.ir.BinaryImplNode;
import org.opensearch.painless.ir.BinaryMathNode;
import org.opensearch.painless.ir.BlockNode;
import org.opensearch.painless.ir.BooleanNode;
import org.opensearch.painless.ir.BreakNode;
import org.opensearch.painless.ir.CastNode;
import org.opensearch.painless.ir.CatchNode;
import org.opensearch.painless.ir.ClassNode;
import org.opensearch.painless.ir.ComparisonNode;
import org.opensearch.painless.ir.ConditionalNode;
import org.opensearch.painless.ir.ConstantNode;
import org.opensearch.painless.ir.ContinueNode;
import org.opensearch.painless.ir.DeclarationBlockNode;
import org.opensearch.painless.ir.DeclarationNode;
import org.opensearch.painless.ir.DefInterfaceReferenceNode;
import org.opensearch.painless.ir.DoWhileLoopNode;
import org.opensearch.painless.ir.DupNode;
import org.opensearch.painless.ir.ElvisNode;
import org.opensearch.painless.ir.FieldNode;
import org.opensearch.painless.ir.FlipArrayIndexNode;
import org.opensearch.painless.ir.FlipCollectionIndexNode;
import org.opensearch.painless.ir.FlipDefIndexNode;
import org.opensearch.painless.ir.ForEachLoopNode;
import org.opensearch.painless.ir.ForEachSubArrayNode;
import org.opensearch.painless.ir.ForEachSubIterableNode;
import org.opensearch.painless.ir.ForLoopNode;
import org.opensearch.painless.ir.FunctionNode;
import org.opensearch.painless.ir.IfElseNode;
import org.opensearch.painless.ir.IfNode;
import org.opensearch.painless.ir.InstanceofNode;
import org.opensearch.painless.ir.InvokeCallDefNode;
import org.opensearch.painless.ir.InvokeCallMemberNode;
import org.opensearch.painless.ir.InvokeCallNode;
import org.opensearch.painless.ir.ListInitializationNode;
import org.opensearch.painless.ir.LoadBraceDefNode;
import org.opensearch.painless.ir.LoadBraceNode;
import org.opensearch.painless.ir.LoadDotArrayLengthNode;
import org.opensearch.painless.ir.LoadDotDefNode;
import org.opensearch.painless.ir.LoadDotNode;
import org.opensearch.painless.ir.LoadDotShortcutNode;
import org.opensearch.painless.ir.LoadFieldMemberNode;
import org.opensearch.painless.ir.LoadListShortcutNode;
import org.opensearch.painless.ir.LoadMapShortcutNode;
import org.opensearch.painless.ir.LoadVariableNode;
import org.opensearch.painless.ir.MapInitializationNode;
import org.opensearch.painless.ir.NewArrayNode;
import org.opensearch.painless.ir.NewObjectNode;
import org.opensearch.painless.ir.NullNode;
import org.opensearch.painless.ir.NullSafeSubNode;
import org.opensearch.painless.ir.ReturnNode;
import org.opensearch.painless.ir.StatementExpressionNode;
import org.opensearch.painless.ir.StaticNode;
import org.opensearch.painless.ir.StoreBraceDefNode;
import org.opensearch.painless.ir.StoreBraceNode;
import org.opensearch.painless.ir.StoreDotDefNode;
import org.opensearch.painless.ir.StoreDotNode;
import org.opensearch.painless.ir.StoreDotShortcutNode;
import org.opensearch.painless.ir.StoreFieldMemberNode;
import org.opensearch.painless.ir.StoreListShortcutNode;
import org.opensearch.painless.ir.StoreMapShortcutNode;
import org.opensearch.painless.ir.StoreVariableNode;
import org.opensearch.painless.ir.StringConcatenationNode;
import org.opensearch.painless.ir.ThrowNode;
import org.opensearch.painless.ir.TryNode;
import org.opensearch.painless.ir.TypedCaptureReferenceNode;
import org.opensearch.painless.ir.TypedInterfaceReferenceNode;
import org.opensearch.painless.ir.UnaryMathNode;
import org.opensearch.painless.ir.WhileLoopNode;

public interface IRTreeVisitor<Scope> {

    void visitClass(ClassNode irClassNode, Scope scope);

    void visitFunction(FunctionNode irFunctionNode, Scope scope);

    void visitField(FieldNode irFieldNode, Scope scope);

    void visitBlock(BlockNode irBlockNode, Scope scope);

    void visitIf(IfNode irIfNode, Scope scope);

    void visitIfElse(IfElseNode irIfElseNode, Scope scope);

    void visitWhileLoop(WhileLoopNode irWhileLoopNode, Scope scope);

    void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, Scope scope);

    void visitForLoop(ForLoopNode irForLoopNode, Scope scope);

    void visitForEachLoop(ForEachLoopNode irForEachLoopNode, Scope scope);

    void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, Scope scope);

    void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, Scope scope);

    void visitDeclarationBlock(DeclarationBlockNode irDeclarationBlockNode, Scope scope);

    void visitDeclaration(DeclarationNode irDeclarationNode, Scope scope);

    void visitReturn(ReturnNode irReturnNode, Scope scope);

    void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, Scope scope);

    void visitTry(TryNode irTryNode, Scope scope);

    void visitCatch(CatchNode irCatchNode, Scope scope);

    void visitThrow(ThrowNode irThrowNode, Scope scope);

    void visitContinue(ContinueNode irContinueNode, Scope scope);

    void visitBreak(BreakNode irBreakNode, Scope scope);

    void visitBinaryImpl(BinaryImplNode irBinaryImplNode, Scope scope);

    void visitUnaryMath(UnaryMathNode irUnaryMathNode, Scope scope);

    void visitBinaryMath(BinaryMathNode irBinaryMathNode, Scope scope);

    void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Scope scope);

    void visitBoolean(BooleanNode irBooleanNode, Scope scope);

    void visitComparison(ComparisonNode irComparisonNode, Scope scope);

    void visitCast(CastNode irCastNode, Scope scope);

    void visitInstanceof(InstanceofNode irInstanceofNode, Scope scope);

    void visitConditional(ConditionalNode irConditionalNode, Scope scope);

    void visitElvis(ElvisNode irElvisNode, Scope scope);

    void visitListInitialization(ListInitializationNode irListInitializationNode, Scope scope);

    void visitMapInitialization(MapInitializationNode irMapInitializationNode, Scope scope);

    void visitNewArray(NewArrayNode irNewArrayNode, Scope scope);

    void visitNewObject(NewObjectNode irNewObjectNode, Scope scope);

    void visitConstant(ConstantNode irConstantNode, Scope scope);

    void visitNull(NullNode irNullNode, Scope scope);

    void visitDefInterfaceReference(DefInterfaceReferenceNode irDefInterfaceReferenceNode, Scope scope);

    void visitTypedInterfaceReference(TypedInterfaceReferenceNode irTypedInterfaceReferenceNode, Scope scope);

    void visitTypeCaptureReference(TypedCaptureReferenceNode irTypedCaptureReferenceNode, Scope scope);

    void visitStatic(StaticNode irStaticNode, Scope scope);

    void visitLoadVariable(LoadVariableNode irLoadVariableNode, Scope scope);

    void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, Scope scope);

    void visitLoadDotArrayLengthNode(LoadDotArrayLengthNode irLoadDotArrayLengthNode, Scope scope);

    void visitLoadDotDef(LoadDotDefNode irLoadDotDefNode, Scope scope);

    void visitLoadDot(LoadDotNode irLoadDotNode, Scope scope);

    void visitLoadDotShortcut(LoadDotShortcutNode irDotSubShortcutNode, Scope scope);

    void visitLoadListShortcut(LoadListShortcutNode irLoadListShortcutNode, Scope scope);

    void visitLoadMapShortcut(LoadMapShortcutNode irLoadMapShortcutNode, Scope scope);

    void visitLoadFieldMember(LoadFieldMemberNode irLoadFieldMemberNode, Scope scope);

    void visitLoadBraceDef(LoadBraceDefNode irLoadBraceDefNode, Scope scope);

    void visitLoadBrace(LoadBraceNode irLoadBraceNode, Scope scope);

    void visitStoreVariable(StoreVariableNode irStoreVariableNode, Scope scope);

    void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, Scope scope);

    void visitStoreDot(StoreDotNode irStoreDotNode, Scope scope);

    void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, Scope scope);

    void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, Scope scope);

    void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, Scope scope);

    void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, Scope scope);

    void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, Scope scope);

    void visitStoreBrace(StoreBraceNode irStoreBraceNode, Scope scope);

    void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, Scope scope);

    void visitInvokeCall(InvokeCallNode irInvokeCallNode, Scope scope);

    void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Scope scope);

    void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, Scope scope);

    void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, Scope scope);

    void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, Scope scope);

    void visitDup(DupNode irDupNode, Scope scope);
}
