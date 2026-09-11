/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.python.antlr;

// Generated from antlr/Python3Parser.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link Python3Parser}.
 */
public interface Python3ParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link Python3Parser#single_input}.
	 * @param ctx the parse tree
	 */
	void enterSingle_input(Python3Parser.Single_inputContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#single_input}.
	 * @param ctx the parse tree
	 */
	void exitSingle_input(Python3Parser.Single_inputContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#file_input}.
	 * @param ctx the parse tree
	 */
	void enterFile_input(Python3Parser.File_inputContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#file_input}.
	 * @param ctx the parse tree
	 */
	void exitFile_input(Python3Parser.File_inputContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#eval_input}.
	 * @param ctx the parse tree
	 */
	void enterEval_input(Python3Parser.Eval_inputContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#eval_input}.
	 * @param ctx the parse tree
	 */
	void exitEval_input(Python3Parser.Eval_inputContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#decorator}.
	 * @param ctx the parse tree
	 */
	void enterDecorator(Python3Parser.DecoratorContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#decorator}.
	 * @param ctx the parse tree
	 */
	void exitDecorator(Python3Parser.DecoratorContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#decorators}.
	 * @param ctx the parse tree
	 */
	void enterDecorators(Python3Parser.DecoratorsContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#decorators}.
	 * @param ctx the parse tree
	 */
	void exitDecorators(Python3Parser.DecoratorsContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#decorated}.
	 * @param ctx the parse tree
	 */
	void enterDecorated(Python3Parser.DecoratedContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#decorated}.
	 * @param ctx the parse tree
	 */
	void exitDecorated(Python3Parser.DecoratedContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#async_funcdef}.
	 * @param ctx the parse tree
	 */
	void enterAsync_funcdef(Python3Parser.Async_funcdefContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#async_funcdef}.
	 * @param ctx the parse tree
	 */
	void exitAsync_funcdef(Python3Parser.Async_funcdefContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#funcdef}.
	 * @param ctx the parse tree
	 */
	void enterFuncdef(Python3Parser.FuncdefContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#funcdef}.
	 * @param ctx the parse tree
	 */
	void exitFuncdef(Python3Parser.FuncdefContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#parameters}.
	 * @param ctx the parse tree
	 */
	void enterParameters(Python3Parser.ParametersContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#parameters}.
	 * @param ctx the parse tree
	 */
	void exitParameters(Python3Parser.ParametersContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#typedargslist}.
	 * @param ctx the parse tree
	 */
	void enterTypedargslist(Python3Parser.TypedargslistContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#typedargslist}.
	 * @param ctx the parse tree
	 */
	void exitTypedargslist(Python3Parser.TypedargslistContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#tfpdef}.
	 * @param ctx the parse tree
	 */
	void enterTfpdef(Python3Parser.TfpdefContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#tfpdef}.
	 * @param ctx the parse tree
	 */
	void exitTfpdef(Python3Parser.TfpdefContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#varargslist}.
	 * @param ctx the parse tree
	 */
	void enterVarargslist(Python3Parser.VarargslistContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#varargslist}.
	 * @param ctx the parse tree
	 */
	void exitVarargslist(Python3Parser.VarargslistContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#vfpdef}.
	 * @param ctx the parse tree
	 */
	void enterVfpdef(Python3Parser.VfpdefContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#vfpdef}.
	 * @param ctx the parse tree
	 */
	void exitVfpdef(Python3Parser.VfpdefContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#stmt}.
	 * @param ctx the parse tree
	 */
	void enterStmt(Python3Parser.StmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#stmt}.
	 * @param ctx the parse tree
	 */
	void exitStmt(Python3Parser.StmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#simple_stmts}.
	 * @param ctx the parse tree
	 */
	void enterSimple_stmts(Python3Parser.Simple_stmtsContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#simple_stmts}.
	 * @param ctx the parse tree
	 */
	void exitSimple_stmts(Python3Parser.Simple_stmtsContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#simple_stmt}.
	 * @param ctx the parse tree
	 */
	void enterSimple_stmt(Python3Parser.Simple_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#simple_stmt}.
	 * @param ctx the parse tree
	 */
	void exitSimple_stmt(Python3Parser.Simple_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#expr_stmt}.
	 * @param ctx the parse tree
	 */
	void enterExpr_stmt(Python3Parser.Expr_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#expr_stmt}.
	 * @param ctx the parse tree
	 */
	void exitExpr_stmt(Python3Parser.Expr_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#annassign}.
	 * @param ctx the parse tree
	 */
	void enterAnnassign(Python3Parser.AnnassignContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#annassign}.
	 * @param ctx the parse tree
	 */
	void exitAnnassign(Python3Parser.AnnassignContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#testlist_star_expr}.
	 * @param ctx the parse tree
	 */
	void enterTestlist_star_expr(Python3Parser.Testlist_star_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#testlist_star_expr}.
	 * @param ctx the parse tree
	 */
	void exitTestlist_star_expr(Python3Parser.Testlist_star_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#augassign}.
	 * @param ctx the parse tree
	 */
	void enterAugassign(Python3Parser.AugassignContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#augassign}.
	 * @param ctx the parse tree
	 */
	void exitAugassign(Python3Parser.AugassignContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#del_stmt}.
	 * @param ctx the parse tree
	 */
	void enterDel_stmt(Python3Parser.Del_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#del_stmt}.
	 * @param ctx the parse tree
	 */
	void exitDel_stmt(Python3Parser.Del_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#pass_stmt}.
	 * @param ctx the parse tree
	 */
	void enterPass_stmt(Python3Parser.Pass_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#pass_stmt}.
	 * @param ctx the parse tree
	 */
	void exitPass_stmt(Python3Parser.Pass_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#flow_stmt}.
	 * @param ctx the parse tree
	 */
	void enterFlow_stmt(Python3Parser.Flow_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#flow_stmt}.
	 * @param ctx the parse tree
	 */
	void exitFlow_stmt(Python3Parser.Flow_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#break_stmt}.
	 * @param ctx the parse tree
	 */
	void enterBreak_stmt(Python3Parser.Break_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#break_stmt}.
	 * @param ctx the parse tree
	 */
	void exitBreak_stmt(Python3Parser.Break_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#continue_stmt}.
	 * @param ctx the parse tree
	 */
	void enterContinue_stmt(Python3Parser.Continue_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#continue_stmt}.
	 * @param ctx the parse tree
	 */
	void exitContinue_stmt(Python3Parser.Continue_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#return_stmt}.
	 * @param ctx the parse tree
	 */
	void enterReturn_stmt(Python3Parser.Return_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#return_stmt}.
	 * @param ctx the parse tree
	 */
	void exitReturn_stmt(Python3Parser.Return_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#yield_stmt}.
	 * @param ctx the parse tree
	 */
	void enterYield_stmt(Python3Parser.Yield_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#yield_stmt}.
	 * @param ctx the parse tree
	 */
	void exitYield_stmt(Python3Parser.Yield_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#raise_stmt}.
	 * @param ctx the parse tree
	 */
	void enterRaise_stmt(Python3Parser.Raise_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#raise_stmt}.
	 * @param ctx the parse tree
	 */
	void exitRaise_stmt(Python3Parser.Raise_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#import_stmt}.
	 * @param ctx the parse tree
	 */
	void enterImport_stmt(Python3Parser.Import_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#import_stmt}.
	 * @param ctx the parse tree
	 */
	void exitImport_stmt(Python3Parser.Import_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#import_name}.
	 * @param ctx the parse tree
	 */
	void enterImport_name(Python3Parser.Import_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#import_name}.
	 * @param ctx the parse tree
	 */
	void exitImport_name(Python3Parser.Import_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#import_from}.
	 * @param ctx the parse tree
	 */
	void enterImport_from(Python3Parser.Import_fromContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#import_from}.
	 * @param ctx the parse tree
	 */
	void exitImport_from(Python3Parser.Import_fromContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#import_as_name}.
	 * @param ctx the parse tree
	 */
	void enterImport_as_name(Python3Parser.Import_as_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#import_as_name}.
	 * @param ctx the parse tree
	 */
	void exitImport_as_name(Python3Parser.Import_as_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#dotted_as_name}.
	 * @param ctx the parse tree
	 */
	void enterDotted_as_name(Python3Parser.Dotted_as_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#dotted_as_name}.
	 * @param ctx the parse tree
	 */
	void exitDotted_as_name(Python3Parser.Dotted_as_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#import_as_names}.
	 * @param ctx the parse tree
	 */
	void enterImport_as_names(Python3Parser.Import_as_namesContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#import_as_names}.
	 * @param ctx the parse tree
	 */
	void exitImport_as_names(Python3Parser.Import_as_namesContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#dotted_as_names}.
	 * @param ctx the parse tree
	 */
	void enterDotted_as_names(Python3Parser.Dotted_as_namesContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#dotted_as_names}.
	 * @param ctx the parse tree
	 */
	void exitDotted_as_names(Python3Parser.Dotted_as_namesContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#dotted_name}.
	 * @param ctx the parse tree
	 */
	void enterDotted_name(Python3Parser.Dotted_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#dotted_name}.
	 * @param ctx the parse tree
	 */
	void exitDotted_name(Python3Parser.Dotted_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#global_stmt}.
	 * @param ctx the parse tree
	 */
	void enterGlobal_stmt(Python3Parser.Global_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#global_stmt}.
	 * @param ctx the parse tree
	 */
	void exitGlobal_stmt(Python3Parser.Global_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#nonlocal_stmt}.
	 * @param ctx the parse tree
	 */
	void enterNonlocal_stmt(Python3Parser.Nonlocal_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#nonlocal_stmt}.
	 * @param ctx the parse tree
	 */
	void exitNonlocal_stmt(Python3Parser.Nonlocal_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#assert_stmt}.
	 * @param ctx the parse tree
	 */
	void enterAssert_stmt(Python3Parser.Assert_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#assert_stmt}.
	 * @param ctx the parse tree
	 */
	void exitAssert_stmt(Python3Parser.Assert_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#compound_stmt}.
	 * @param ctx the parse tree
	 */
	void enterCompound_stmt(Python3Parser.Compound_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#compound_stmt}.
	 * @param ctx the parse tree
	 */
	void exitCompound_stmt(Python3Parser.Compound_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#async_stmt}.
	 * @param ctx the parse tree
	 */
	void enterAsync_stmt(Python3Parser.Async_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#async_stmt}.
	 * @param ctx the parse tree
	 */
	void exitAsync_stmt(Python3Parser.Async_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#if_stmt}.
	 * @param ctx the parse tree
	 */
	void enterIf_stmt(Python3Parser.If_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#if_stmt}.
	 * @param ctx the parse tree
	 */
	void exitIf_stmt(Python3Parser.If_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#while_stmt}.
	 * @param ctx the parse tree
	 */
	void enterWhile_stmt(Python3Parser.While_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#while_stmt}.
	 * @param ctx the parse tree
	 */
	void exitWhile_stmt(Python3Parser.While_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#for_stmt}.
	 * @param ctx the parse tree
	 */
	void enterFor_stmt(Python3Parser.For_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#for_stmt}.
	 * @param ctx the parse tree
	 */
	void exitFor_stmt(Python3Parser.For_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#try_stmt}.
	 * @param ctx the parse tree
	 */
	void enterTry_stmt(Python3Parser.Try_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#try_stmt}.
	 * @param ctx the parse tree
	 */
	void exitTry_stmt(Python3Parser.Try_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#with_stmt}.
	 * @param ctx the parse tree
	 */
	void enterWith_stmt(Python3Parser.With_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#with_stmt}.
	 * @param ctx the parse tree
	 */
	void exitWith_stmt(Python3Parser.With_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#with_item}.
	 * @param ctx the parse tree
	 */
	void enterWith_item(Python3Parser.With_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#with_item}.
	 * @param ctx the parse tree
	 */
	void exitWith_item(Python3Parser.With_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#except_clause}.
	 * @param ctx the parse tree
	 */
	void enterExcept_clause(Python3Parser.Except_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#except_clause}.
	 * @param ctx the parse tree
	 */
	void exitExcept_clause(Python3Parser.Except_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#block}.
	 * @param ctx the parse tree
	 */
	void enterBlock(Python3Parser.BlockContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#block}.
	 * @param ctx the parse tree
	 */
	void exitBlock(Python3Parser.BlockContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#match_stmt}.
	 * @param ctx the parse tree
	 */
	void enterMatch_stmt(Python3Parser.Match_stmtContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#match_stmt}.
	 * @param ctx the parse tree
	 */
	void exitMatch_stmt(Python3Parser.Match_stmtContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#subject_expr}.
	 * @param ctx the parse tree
	 */
	void enterSubject_expr(Python3Parser.Subject_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#subject_expr}.
	 * @param ctx the parse tree
	 */
	void exitSubject_expr(Python3Parser.Subject_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#star_named_expressions}.
	 * @param ctx the parse tree
	 */
	void enterStar_named_expressions(Python3Parser.Star_named_expressionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#star_named_expressions}.
	 * @param ctx the parse tree
	 */
	void exitStar_named_expressions(Python3Parser.Star_named_expressionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#star_named_expression}.
	 * @param ctx the parse tree
	 */
	void enterStar_named_expression(Python3Parser.Star_named_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#star_named_expression}.
	 * @param ctx the parse tree
	 */
	void exitStar_named_expression(Python3Parser.Star_named_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#case_block}.
	 * @param ctx the parse tree
	 */
	void enterCase_block(Python3Parser.Case_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#case_block}.
	 * @param ctx the parse tree
	 */
	void exitCase_block(Python3Parser.Case_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#guard}.
	 * @param ctx the parse tree
	 */
	void enterGuard(Python3Parser.GuardContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#guard}.
	 * @param ctx the parse tree
	 */
	void exitGuard(Python3Parser.GuardContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#patterns}.
	 * @param ctx the parse tree
	 */
	void enterPatterns(Python3Parser.PatternsContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#patterns}.
	 * @param ctx the parse tree
	 */
	void exitPatterns(Python3Parser.PatternsContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#pattern}.
	 * @param ctx the parse tree
	 */
	void enterPattern(Python3Parser.PatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#pattern}.
	 * @param ctx the parse tree
	 */
	void exitPattern(Python3Parser.PatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#as_pattern}.
	 * @param ctx the parse tree
	 */
	void enterAs_pattern(Python3Parser.As_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#as_pattern}.
	 * @param ctx the parse tree
	 */
	void exitAs_pattern(Python3Parser.As_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#or_pattern}.
	 * @param ctx the parse tree
	 */
	void enterOr_pattern(Python3Parser.Or_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#or_pattern}.
	 * @param ctx the parse tree
	 */
	void exitOr_pattern(Python3Parser.Or_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#closed_pattern}.
	 * @param ctx the parse tree
	 */
	void enterClosed_pattern(Python3Parser.Closed_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#closed_pattern}.
	 * @param ctx the parse tree
	 */
	void exitClosed_pattern(Python3Parser.Closed_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#literal_pattern}.
	 * @param ctx the parse tree
	 */
	void enterLiteral_pattern(Python3Parser.Literal_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#literal_pattern}.
	 * @param ctx the parse tree
	 */
	void exitLiteral_pattern(Python3Parser.Literal_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#literal_expr}.
	 * @param ctx the parse tree
	 */
	void enterLiteral_expr(Python3Parser.Literal_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#literal_expr}.
	 * @param ctx the parse tree
	 */
	void exitLiteral_expr(Python3Parser.Literal_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#complex_number}.
	 * @param ctx the parse tree
	 */
	void enterComplex_number(Python3Parser.Complex_numberContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#complex_number}.
	 * @param ctx the parse tree
	 */
	void exitComplex_number(Python3Parser.Complex_numberContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#signed_number}.
	 * @param ctx the parse tree
	 */
	void enterSigned_number(Python3Parser.Signed_numberContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#signed_number}.
	 * @param ctx the parse tree
	 */
	void exitSigned_number(Python3Parser.Signed_numberContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#signed_real_number}.
	 * @param ctx the parse tree
	 */
	void enterSigned_real_number(Python3Parser.Signed_real_numberContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#signed_real_number}.
	 * @param ctx the parse tree
	 */
	void exitSigned_real_number(Python3Parser.Signed_real_numberContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#real_number}.
	 * @param ctx the parse tree
	 */
	void enterReal_number(Python3Parser.Real_numberContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#real_number}.
	 * @param ctx the parse tree
	 */
	void exitReal_number(Python3Parser.Real_numberContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#imaginary_number}.
	 * @param ctx the parse tree
	 */
	void enterImaginary_number(Python3Parser.Imaginary_numberContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#imaginary_number}.
	 * @param ctx the parse tree
	 */
	void exitImaginary_number(Python3Parser.Imaginary_numberContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#capture_pattern}.
	 * @param ctx the parse tree
	 */
	void enterCapture_pattern(Python3Parser.Capture_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#capture_pattern}.
	 * @param ctx the parse tree
	 */
	void exitCapture_pattern(Python3Parser.Capture_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#pattern_capture_target}.
	 * @param ctx the parse tree
	 */
	void enterPattern_capture_target(Python3Parser.Pattern_capture_targetContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#pattern_capture_target}.
	 * @param ctx the parse tree
	 */
	void exitPattern_capture_target(Python3Parser.Pattern_capture_targetContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#wildcard_pattern}.
	 * @param ctx the parse tree
	 */
	void enterWildcard_pattern(Python3Parser.Wildcard_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#wildcard_pattern}.
	 * @param ctx the parse tree
	 */
	void exitWildcard_pattern(Python3Parser.Wildcard_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#value_pattern}.
	 * @param ctx the parse tree
	 */
	void enterValue_pattern(Python3Parser.Value_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#value_pattern}.
	 * @param ctx the parse tree
	 */
	void exitValue_pattern(Python3Parser.Value_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#attr}.
	 * @param ctx the parse tree
	 */
	void enterAttr(Python3Parser.AttrContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#attr}.
	 * @param ctx the parse tree
	 */
	void exitAttr(Python3Parser.AttrContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#name_or_attr}.
	 * @param ctx the parse tree
	 */
	void enterName_or_attr(Python3Parser.Name_or_attrContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#name_or_attr}.
	 * @param ctx the parse tree
	 */
	void exitName_or_attr(Python3Parser.Name_or_attrContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#group_pattern}.
	 * @param ctx the parse tree
	 */
	void enterGroup_pattern(Python3Parser.Group_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#group_pattern}.
	 * @param ctx the parse tree
	 */
	void exitGroup_pattern(Python3Parser.Group_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#sequence_pattern}.
	 * @param ctx the parse tree
	 */
	void enterSequence_pattern(Python3Parser.Sequence_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#sequence_pattern}.
	 * @param ctx the parse tree
	 */
	void exitSequence_pattern(Python3Parser.Sequence_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#open_sequence_pattern}.
	 * @param ctx the parse tree
	 */
	void enterOpen_sequence_pattern(Python3Parser.Open_sequence_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#open_sequence_pattern}.
	 * @param ctx the parse tree
	 */
	void exitOpen_sequence_pattern(Python3Parser.Open_sequence_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#maybe_sequence_pattern}.
	 * @param ctx the parse tree
	 */
	void enterMaybe_sequence_pattern(Python3Parser.Maybe_sequence_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#maybe_sequence_pattern}.
	 * @param ctx the parse tree
	 */
	void exitMaybe_sequence_pattern(Python3Parser.Maybe_sequence_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#maybe_star_pattern}.
	 * @param ctx the parse tree
	 */
	void enterMaybe_star_pattern(Python3Parser.Maybe_star_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#maybe_star_pattern}.
	 * @param ctx the parse tree
	 */
	void exitMaybe_star_pattern(Python3Parser.Maybe_star_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#star_pattern}.
	 * @param ctx the parse tree
	 */
	void enterStar_pattern(Python3Parser.Star_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#star_pattern}.
	 * @param ctx the parse tree
	 */
	void exitStar_pattern(Python3Parser.Star_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#mapping_pattern}.
	 * @param ctx the parse tree
	 */
	void enterMapping_pattern(Python3Parser.Mapping_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#mapping_pattern}.
	 * @param ctx the parse tree
	 */
	void exitMapping_pattern(Python3Parser.Mapping_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#items_pattern}.
	 * @param ctx the parse tree
	 */
	void enterItems_pattern(Python3Parser.Items_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#items_pattern}.
	 * @param ctx the parse tree
	 */
	void exitItems_pattern(Python3Parser.Items_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#key_value_pattern}.
	 * @param ctx the parse tree
	 */
	void enterKey_value_pattern(Python3Parser.Key_value_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#key_value_pattern}.
	 * @param ctx the parse tree
	 */
	void exitKey_value_pattern(Python3Parser.Key_value_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#double_star_pattern}.
	 * @param ctx the parse tree
	 */
	void enterDouble_star_pattern(Python3Parser.Double_star_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#double_star_pattern}.
	 * @param ctx the parse tree
	 */
	void exitDouble_star_pattern(Python3Parser.Double_star_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#class_pattern}.
	 * @param ctx the parse tree
	 */
	void enterClass_pattern(Python3Parser.Class_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#class_pattern}.
	 * @param ctx the parse tree
	 */
	void exitClass_pattern(Python3Parser.Class_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#positional_patterns}.
	 * @param ctx the parse tree
	 */
	void enterPositional_patterns(Python3Parser.Positional_patternsContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#positional_patterns}.
	 * @param ctx the parse tree
	 */
	void exitPositional_patterns(Python3Parser.Positional_patternsContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#keyword_patterns}.
	 * @param ctx the parse tree
	 */
	void enterKeyword_patterns(Python3Parser.Keyword_patternsContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#keyword_patterns}.
	 * @param ctx the parse tree
	 */
	void exitKeyword_patterns(Python3Parser.Keyword_patternsContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#keyword_pattern}.
	 * @param ctx the parse tree
	 */
	void enterKeyword_pattern(Python3Parser.Keyword_patternContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#keyword_pattern}.
	 * @param ctx the parse tree
	 */
	void exitKeyword_pattern(Python3Parser.Keyword_patternContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#test}.
	 * @param ctx the parse tree
	 */
	void enterTest(Python3Parser.TestContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#test}.
	 * @param ctx the parse tree
	 */
	void exitTest(Python3Parser.TestContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#test_nocond}.
	 * @param ctx the parse tree
	 */
	void enterTest_nocond(Python3Parser.Test_nocondContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#test_nocond}.
	 * @param ctx the parse tree
	 */
	void exitTest_nocond(Python3Parser.Test_nocondContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#lambdef}.
	 * @param ctx the parse tree
	 */
	void enterLambdef(Python3Parser.LambdefContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#lambdef}.
	 * @param ctx the parse tree
	 */
	void exitLambdef(Python3Parser.LambdefContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#lambdef_nocond}.
	 * @param ctx the parse tree
	 */
	void enterLambdef_nocond(Python3Parser.Lambdef_nocondContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#lambdef_nocond}.
	 * @param ctx the parse tree
	 */
	void exitLambdef_nocond(Python3Parser.Lambdef_nocondContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#or_test}.
	 * @param ctx the parse tree
	 */
	void enterOr_test(Python3Parser.Or_testContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#or_test}.
	 * @param ctx the parse tree
	 */
	void exitOr_test(Python3Parser.Or_testContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#and_test}.
	 * @param ctx the parse tree
	 */
	void enterAnd_test(Python3Parser.And_testContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#and_test}.
	 * @param ctx the parse tree
	 */
	void exitAnd_test(Python3Parser.And_testContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#not_test}.
	 * @param ctx the parse tree
	 */
	void enterNot_test(Python3Parser.Not_testContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#not_test}.
	 * @param ctx the parse tree
	 */
	void exitNot_test(Python3Parser.Not_testContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#comparison}.
	 * @param ctx the parse tree
	 */
	void enterComparison(Python3Parser.ComparisonContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#comparison}.
	 * @param ctx the parse tree
	 */
	void exitComparison(Python3Parser.ComparisonContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#comp_op}.
	 * @param ctx the parse tree
	 */
	void enterComp_op(Python3Parser.Comp_opContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#comp_op}.
	 * @param ctx the parse tree
	 */
	void exitComp_op(Python3Parser.Comp_opContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#star_expr}.
	 * @param ctx the parse tree
	 */
	void enterStar_expr(Python3Parser.Star_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#star_expr}.
	 * @param ctx the parse tree
	 */
	void exitStar_expr(Python3Parser.Star_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExpr(Python3Parser.ExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExpr(Python3Parser.ExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#atom_expr}.
	 * @param ctx the parse tree
	 */
	void enterAtom_expr(Python3Parser.Atom_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#atom_expr}.
	 * @param ctx the parse tree
	 */
	void exitAtom_expr(Python3Parser.Atom_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#atom}.
	 * @param ctx the parse tree
	 */
	void enterAtom(Python3Parser.AtomContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#atom}.
	 * @param ctx the parse tree
	 */
	void exitAtom(Python3Parser.AtomContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#name}.
	 * @param ctx the parse tree
	 */
	void enterName(Python3Parser.NameContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#name}.
	 * @param ctx the parse tree
	 */
	void exitName(Python3Parser.NameContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#testlist_comp}.
	 * @param ctx the parse tree
	 */
	void enterTestlist_comp(Python3Parser.Testlist_compContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#testlist_comp}.
	 * @param ctx the parse tree
	 */
	void exitTestlist_comp(Python3Parser.Testlist_compContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#trailer}.
	 * @param ctx the parse tree
	 */
	void enterTrailer(Python3Parser.TrailerContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#trailer}.
	 * @param ctx the parse tree
	 */
	void exitTrailer(Python3Parser.TrailerContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#subscriptlist}.
	 * @param ctx the parse tree
	 */
	void enterSubscriptlist(Python3Parser.SubscriptlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#subscriptlist}.
	 * @param ctx the parse tree
	 */
	void exitSubscriptlist(Python3Parser.SubscriptlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#subscript_}.
	 * @param ctx the parse tree
	 */
	void enterSubscript_(Python3Parser.Subscript_Context ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#subscript_}.
	 * @param ctx the parse tree
	 */
	void exitSubscript_(Python3Parser.Subscript_Context ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#sliceop}.
	 * @param ctx the parse tree
	 */
	void enterSliceop(Python3Parser.SliceopContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#sliceop}.
	 * @param ctx the parse tree
	 */
	void exitSliceop(Python3Parser.SliceopContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#exprlist}.
	 * @param ctx the parse tree
	 */
	void enterExprlist(Python3Parser.ExprlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#exprlist}.
	 * @param ctx the parse tree
	 */
	void exitExprlist(Python3Parser.ExprlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#testlist}.
	 * @param ctx the parse tree
	 */
	void enterTestlist(Python3Parser.TestlistContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#testlist}.
	 * @param ctx the parse tree
	 */
	void exitTestlist(Python3Parser.TestlistContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#dictorsetmaker}.
	 * @param ctx the parse tree
	 */
	void enterDictorsetmaker(Python3Parser.DictorsetmakerContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#dictorsetmaker}.
	 * @param ctx the parse tree
	 */
	void exitDictorsetmaker(Python3Parser.DictorsetmakerContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#classdef}.
	 * @param ctx the parse tree
	 */
	void enterClassdef(Python3Parser.ClassdefContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#classdef}.
	 * @param ctx the parse tree
	 */
	void exitClassdef(Python3Parser.ClassdefContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#arglist}.
	 * @param ctx the parse tree
	 */
	void enterArglist(Python3Parser.ArglistContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#arglist}.
	 * @param ctx the parse tree
	 */
	void exitArglist(Python3Parser.ArglistContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#argument}.
	 * @param ctx the parse tree
	 */
	void enterArgument(Python3Parser.ArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#argument}.
	 * @param ctx the parse tree
	 */
	void exitArgument(Python3Parser.ArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#comp_iter}.
	 * @param ctx the parse tree
	 */
	void enterComp_iter(Python3Parser.Comp_iterContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#comp_iter}.
	 * @param ctx the parse tree
	 */
	void exitComp_iter(Python3Parser.Comp_iterContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#comp_for}.
	 * @param ctx the parse tree
	 */
	void enterComp_for(Python3Parser.Comp_forContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#comp_for}.
	 * @param ctx the parse tree
	 */
	void exitComp_for(Python3Parser.Comp_forContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#comp_if}.
	 * @param ctx the parse tree
	 */
	void enterComp_if(Python3Parser.Comp_ifContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#comp_if}.
	 * @param ctx the parse tree
	 */
	void exitComp_if(Python3Parser.Comp_ifContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#encoding_decl}.
	 * @param ctx the parse tree
	 */
	void enterEncoding_decl(Python3Parser.Encoding_declContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#encoding_decl}.
	 * @param ctx the parse tree
	 */
	void exitEncoding_decl(Python3Parser.Encoding_declContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#yield_expr}.
	 * @param ctx the parse tree
	 */
	void enterYield_expr(Python3Parser.Yield_exprContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#yield_expr}.
	 * @param ctx the parse tree
	 */
	void exitYield_expr(Python3Parser.Yield_exprContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#yield_arg}.
	 * @param ctx the parse tree
	 */
	void enterYield_arg(Python3Parser.Yield_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#yield_arg}.
	 * @param ctx the parse tree
	 */
	void exitYield_arg(Python3Parser.Yield_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link Python3Parser#strings}.
	 * @param ctx the parse tree
	 */
	void enterStrings(Python3Parser.StringsContext ctx);
	/**
	 * Exit a parse tree produced by {@link Python3Parser#strings}.
	 * @param ctx the parse tree
	 */
	void exitStrings(Python3Parser.StringsContext ctx);
}
