/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.MakeStructFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Rebuilds OpenSearch {@code object} fields as structs in a project directly above the table scan.
 *
 * <p>An object is stored as flat dotted leaf columns and has no physical column of its own —
 * {@code FieldStorageResolver} recurses past object parents because "object fields themselves have
 * no storage" — while the schema still exposes it as a ROW column so queries can name it. So this
 * strips struct columns from the scan and reassembles them above it:
 *
 * <pre>
 * LogicalProject(id=[$0], meta.top=[$1], meta.props.name=[$2],
 *                meta=[make_struct('top', $1, 'props', make_struct('name', $2))])
 *   LogicalTableScan(table=[[t]])      // leaves only
 * </pre>
 *
 * <p>The project reproduces the scan's original row type exactly, so every {@code RexInputRef}
 * above stays valid. Sub-objects nest another {@code make_struct}, so any depth resolves in one
 * pass.
 *
 * <p>One-shot pass rather than a HEP rule: a rule matching {@code TableScan} and producing
 * {@code Project(TableScan)} would re-match its own output.
 *
 * <p>Must run before {@code trimFields}. It emits a {@code make_struct} per object the scan
 * declares, and the trimmer drops the unreferenced ones — otherwise a query filtering on one leaf
 * pays for, and can fail on, an unrelated object. Leaf pushdown still works, since the leaves stay
 * in this project's output and {@code FILTER_PROJECT_TRANSPOSE} moves filters through it.
 *
 * <p><b>TODO: store objects as native Parquet structs instead of flat dotted leaves.</b> A Parquet
 * {@code STRUCT} yields the same leaf column chunks we write today, so pruning and encoding are
 * unchanged — the only difference is a group node in the schema. The reader would then hand the
 * object back assembled, making this pass, {@code MakeStructFunction}, and
 * {@code MakeStructCallConverter} unnecessary for {@code object} support. Keep {@code make_struct}
 * regardless: reconstructing {@code nested} from parallel {@code LIST<T>} leaves needs it.
 *
 * <p>Blocked on the Rust merge path, which handles only top-level columns —
 * {@code build_parquet_root_schema} unions segment schemas by top-level name with first-writer-wins,
 * and {@code ColumnMapping} never null-fills a missing struct child, so a struct gaining a sub-field
 * between segments fails the merge. The writer also has no {@code Struct} in
 * {@code ParquetSettings.ARROW_TYPE_NAME_TO_INSTANCE}.
 *
 * <p>What changes on the search side, since it is more than deleting this class. Native storage
 * gives the scan the ROW column <em>only</em> — there are no {@code city.name} columns — so every
 * consumer of the dotted-leaf convention moves:
 *
 * <ul>
 *   <li>{@code OpenSearchSchemaBuilder} adds flat leaf columns today; they would become derived
 *       names resolving to struct children.</li>
 *   <li>{@code FieldStorageResolver} keys {@code fieldStorage} on the same dotted paths, built
 *       independently of the schema builder — the two agree by convention, not by contract. Keeping
 *       that naming stable is what decides whether this migration is cheap: if dotted names still
 *       resolve (to struct children instead of columns), nothing above those two files changes.</li>
 *   <li>{@link Materializer#resolveLeaf} becomes {@code GET_FIELD}; see its javadoc.</li>
 *   <li>Leaf predicate pushdown pushes a field access rather than a column reference.</li>
 *   <li>The plan-shape goldens in {@code ObjectStructPlanShapeTests} pin flat-leaf plans and will
 *       all change. Expected, not a regression.</li>
 *   <li>The companion rewrite noted for pushing struct-authored predicates down
 *       ({@code GET_FIELD(struct, 'x') → leaf ref}) becomes unnecessary — native storage wants the
 *       opposite direction.</li>
 * </ul>
 *
 * <p><b>Two constraints for the doc-values work.</b> Once lucene declares a DocValues scan
 * capability rather than only {@code Index}:
 *
 * <ul>
 *   <li>{@code OpenSearchTableScanRule}'s hardcoded {@code metadataOnlyDriver = "lucene"} exemption
 *       becomes wrong — it exempts lucene from the strict per-field check on the assumption that it
 *       is metadata-only, so a value-producing lucene would stay viable for fields it cannot scan.
 *       That rule's own TODO already calls for a first-class metadata-driver marker; this is on the
 *       critical path for doc values.</li>
 *   <li>The lucene backend declares no project or scalar-function capabilities at all, and
 *       {@code MAKE_STRUCT} is registered only on the DataFusion side. So once leaves become
 *       lucene-scannable, {@code fields city} has a scan lucene could serve but a project only
 *       DataFusion can — either lucene must declare {@code MAKE_STRUCT} or object queries need to be
 *       pinned to a DataFusion-capable fragment.</li>
 * </ul>
 *
 * <p>Acceptance tests already exist and are storage-agnostic: {@code ObjectFieldIT},
 * {@code ApmServiceMapObjectIT}, and {@code ObjectFieldMultiShardIT} assert PPL results — nested
 * JSON shape, types, group counts — not plan shapes, so they should pass unchanged.
 *
 * @opensearch.internal
 */
public final class ObjectStructMaterializer {

    private ObjectStructMaterializer() {}

    /**
     * Rewrites scans that expose struct-typed columns into a leaf-only scan plus a
     * struct-materializing project.
     *
     * @return the rewritten plan, or {@link Optional#empty()} when the plan has no object
     *         columns (callers keep the original plan unchanged)
     */
    public static Optional<RelNode> rewrite(RelNode root) {
        Materializer materializer = new Materializer();
        RelNode rewritten = root.accept(materializer);
        return materializer.changed ? Optional.of(rewritten) : Optional.empty();
    }

    private static final class Materializer extends RelShuttleImpl {

        private boolean changed = false;

        @Override
        public RelNode visit(TableScan scan) {
            RelDataType originalRowType = scan.getRowType();
            List<RelDataTypeField> originalFields = originalRowType.getFieldList();
            if (originalFields.stream().noneMatch(f -> f.getType().isStruct())) {
                return scan;
            }

            // Scan keeps only physically-stored (non-struct) columns.
            RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
            RelDataTypeFactory.Builder leafTypeBuilder = typeFactory.builder();
            Map<String, Integer> leafIndexByName = new HashMap<>();
            for (RelDataTypeField field : originalFields) {
                if (field.getType().isStruct()) {
                    continue;
                }
                leafIndexByName.put(field.getName(), leafTypeBuilder.getFieldCount());
                leafTypeBuilder.add(field.getName(), field.getType());
            }
            RelDataType leafRowType = leafTypeBuilder.build();
            // Strip the struct even when it leaves nothing behind. An index whose only mapped
            // fields are objects has no leaves at all, and a zero-column scan is fine — leaving the
            // struct in instead makes OpenSearchTableScanRule fail with "No backend can scan all
            // requested fields", since the column has no storage for any backend to claim.

            RelOptTable leafTable = new LeafOnlyTable(scan.getTable(), leafRowType);
            RelNode leafScan = LogicalTableScan.create(scan.getCluster(), leafTable, scan.getHints());

            // Rebuild the original row type: leaves pass through, structs assemble in place.
            RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
            List<RexNode> projects = new ArrayList<>(originalFields.size());
            List<String> names = new ArrayList<>(originalFields.size());
            for (RelDataTypeField field : originalFields) {
                names.add(field.getName());
                if (field.getType().isStruct()) {
                    RexNode struct = buildStruct(rexBuilder, leafScan, field.getName(), field.getType(), leafIndexByName);
                    if (struct == null) {
                        // A backing leaf is absent — typed NULL, never a partial struct.
                        projects.add(rexBuilder.makeNullLiteral(field.getType()));
                    } else {
                        projects.add(struct);
                    }
                } else {
                    projects.add(rexBuilder.makeInputRef(leafScan, leafIndexByName.get(field.getName())));
                }
            }

            changed = true;
            return LogicalProject.create(leafScan, List.of(), projects, names);
        }

        /**
         * Recursively builds {@code make_struct} for {@code structType}, resolving each leaf to the
         * trimmed scan's column named {@code path + "." + fieldName}. Returns {@code null} when any
         * leaf is missing, signaling the caller to skip materialization for this column.
         */
        private static RexNode buildStruct(
            RexBuilder rexBuilder,
            RelNode leafScan,
            String path,
            RelDataType structType,
            Map<String, Integer> leafIndexByName
        ) {
            List<String> fieldNames = new ArrayList<>();
            List<RexNode> fieldValues = new ArrayList<>();
            for (RelDataTypeField child : structType.getFieldList()) {
                String childPath = path + "." + child.getName();
                RexNode value;
                if (child.getType().isStruct()) {
                    value = buildStruct(rexBuilder, leafScan, childPath, child.getType(), leafIndexByName);
                } else {
                    value = resolveLeaf(rexBuilder, leafScan, childPath, leafIndexByName);
                }
                if (value == null) {
                    return null;
                }
                fieldNames.add(child.getName());
                fieldValues.add(value);
            }
            if (fieldNames.isEmpty()) {
                return null;
            }
            return MakeStructFunction.makeCall(rexBuilder, structType, fieldNames, fieldValues);
        }

        /**
         * Produces the value of one object leaf, given its dotted path. The single point where this
         * pass depends on leaves being physical columns named by their dotted path.
         *
         * <p>Deliberately isolated: if objects are ever stored as native Parquet structs (see the
         * TODO on this class), the leaf is no longer a column of its own and this becomes
         * {@code GET_FIELD(structRef, name)}. Swapping one implementation is the whole change on this
         * side; the recursion above it doesn't move.
         *
         * @return {@code null} when the leaf has no backing column, which tells the caller to skip
         *         materializing this object rather than emit a partial struct
         */
        private static RexNode resolveLeaf(
            RexBuilder rexBuilder,
            RelNode leafScan,
            String dottedPath,
            Map<String, Integer> leafIndexByName
        ) {
            Integer leafIndex = leafIndexByName.get(dottedPath);
            return leafIndex == null ? null : rexBuilder.makeInputRef(leafScan, leafIndex);
        }
    }

    /**
     * Wraps the scanned table with a row type stripped of struct columns, so downstream physical
     * resolution ({@code FieldStorageResolver}) only ever sees fields that actually have storage.
     * Mirrors the {@code IndexNameTable} wrapper in {@code OpenSearchTableScanRule}.
     */
    private static final class LeafOnlyTable extends RelOptAbstractTable {

        private final RelOptTable delegate;

        LeafOnlyTable(RelOptTable delegate, RelDataType leafRowType) {
            super(delegate.getRelOptSchema(), delegate.getQualifiedName().getLast(), leafRowType);
            this.delegate = delegate;
        }

        @Override
        public List<String> getQualifiedName() {
            // Preserve the original qualified name: OpenSearchTableScanRule resolves the index
            // from it, and RelOptAbstractTable would otherwise report a single-segment name.
            return delegate.getQualifiedName();
        }

        @Override
        public double getRowCount() {
            return delegate.getRowCount();
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            T unwrapped = super.unwrap(clazz);
            return unwrapped != null ? unwrapped : delegate.unwrap(clazz);
        }
    }
}
