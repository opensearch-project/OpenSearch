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

import org.opensearch.painless.node.EAssignment;
import org.opensearch.painless.node.EBinary;
import org.opensearch.painless.node.EBooleanComp;
import org.opensearch.painless.node.EBooleanConstant;
import org.opensearch.painless.node.EBrace;
import org.opensearch.painless.node.ECall;
import org.opensearch.painless.node.ECallLocal;
import org.opensearch.painless.node.EComp;
import org.opensearch.painless.node.EConditional;
import org.opensearch.painless.node.EDecimal;
import org.opensearch.painless.node.EDot;
import org.opensearch.painless.node.EElvis;
import org.opensearch.painless.node.EExplicit;
import org.opensearch.painless.node.EFunctionRef;
import org.opensearch.painless.node.EInstanceof;
import org.opensearch.painless.node.ELambda;
import org.opensearch.painless.node.EListInit;
import org.opensearch.painless.node.EMapInit;
import org.opensearch.painless.node.ENewArray;
import org.opensearch.painless.node.ENewArrayFunctionRef;
import org.opensearch.painless.node.ENewObj;
import org.opensearch.painless.node.ENull;
import org.opensearch.painless.node.ENumeric;
import org.opensearch.painless.node.ERegex;
import org.opensearch.painless.node.EString;
import org.opensearch.painless.node.ESymbol;
import org.opensearch.painless.node.EUnary;
import org.opensearch.painless.node.SBlock;
import org.opensearch.painless.node.SBreak;
import org.opensearch.painless.node.SCatch;
import org.opensearch.painless.node.SClass;
import org.opensearch.painless.node.SContinue;
import org.opensearch.painless.node.SDeclBlock;
import org.opensearch.painless.node.SDeclaration;
import org.opensearch.painless.node.SDo;
import org.opensearch.painless.node.SEach;
import org.opensearch.painless.node.SExpression;
import org.opensearch.painless.node.SFor;
import org.opensearch.painless.node.SFunction;
import org.opensearch.painless.node.SIf;
import org.opensearch.painless.node.SIfElse;
import org.opensearch.painless.node.SReturn;
import org.opensearch.painless.node.SThrow;
import org.opensearch.painless.node.STry;
import org.opensearch.painless.node.SWhile;

public interface UserTreeVisitor<Scope> {

    void visitClass(SClass userClassNode, Scope scope);

    void visitFunction(SFunction userFunctionNode, Scope scope);

    void visitBlock(SBlock userBlockNode, Scope scope);

    void visitIf(SIf userIfNode, Scope scope);

    void visitIfElse(SIfElse userIfElseNode, Scope scope);

    void visitWhile(SWhile userWhileNode, Scope scope);

    void visitDo(SDo userDoNode, Scope scope);

    void visitFor(SFor userForNode, Scope scope);

    void visitEach(SEach userEachNode, Scope scope);

    void visitDeclBlock(SDeclBlock userDeclBlockNode, Scope scope);

    void visitDeclaration(SDeclaration userDeclarationNode, Scope scope);

    void visitReturn(SReturn userReturnNode, Scope scope);

    void visitExpression(SExpression userExpressionNode, Scope scope);

    void visitTry(STry userTryNode, Scope scope);

    void visitCatch(SCatch userCatchNode, Scope scope);

    void visitThrow(SThrow userThrowNode, Scope scope);

    void visitContinue(SContinue userContinueNode, Scope scope);

    void visitBreak(SBreak userBreakNode, Scope scope);

    void visitAssignment(EAssignment userAssignmentNode, Scope scope);

    void visitUnary(EUnary userUnaryNode, Scope scope);

    void visitBinary(EBinary userBinaryNode, Scope scope);

    void visitBooleanComp(EBooleanComp userBooleanCompNode, Scope scope);

    void visitComp(EComp userCompNode, Scope scope);

    void visitExplicit(EExplicit userExplicitNode, Scope scope);

    void visitInstanceof(EInstanceof userInstanceofNode, Scope scope);

    void visitConditional(EConditional userConditionalNode, Scope scope);

    void visitElvis(EElvis userElvisNode, Scope scope);

    void visitListInit(EListInit userListInitNode, Scope scope);

    void visitMapInit(EMapInit userMapInitNode, Scope scope);

    void visitNewArray(ENewArray userNewArrayNode, Scope scope);

    void visitNewObj(ENewObj userNewObjectNode, Scope scope);

    void visitCallLocal(ECallLocal userCallLocalNode, Scope scope);

    void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, Scope scope);

    void visitNumeric(ENumeric userNumericNode, Scope scope);

    void visitDecimal(EDecimal userDecimalNode, Scope scope);

    void visitString(EString userStringNode, Scope scope);

    void visitNull(ENull userNullNode, Scope scope);

    void visitRegex(ERegex userRegexNode, Scope scope);

    void visitLambda(ELambda userLambdaNode, Scope scope);

    void visitFunctionRef(EFunctionRef userFunctionRefNode, Scope scope);

    void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, Scope scope);

    void visitSymbol(ESymbol userSymbolNode, Scope scope);

    void visitDot(EDot userDotNode, Scope scope);

    void visitBrace(EBrace userBraceNode, Scope scope);

    void visitCall(ECall userCallNode, Scope scope);
}
