package util.arrow;

import calcite.operators.LogicalArrowTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

/**
 * Class for encoding an optimised {@link RelNode} query plan into the Aethra Engine Plan Format.
 */
public class AethraQueryEncoder {

    private static final int expectedPlanLength = 1024; // TODO: pick appropriate value

    /**
     * Method which translates the provided query plan into the Aethra Engine Plan Format.
     * @param queryRoot The query to translate.
     * @return The string containing the query plan.
     */
    public static String encode(RelNode queryRoot) {
        StringBuilder builder = new StringBuilder(expectedPlanLength);
        int currentLineIndex = 0;
        currentLineIndex = encode(queryRoot, builder, currentLineIndex);
        return builder.toString();
    }

    private static int encode(RelNode operator, StringBuilder builder, int currentLineIndex) {
        // Forward the call to the appropriate operator encoder
        if (operator instanceof LogicalArrowTableScan lt)
            currentLineIndex = encode(lt, builder, currentLineIndex);

        else if (operator instanceof LogicalAggregate la)
            currentLineIndex = encode(la, builder, currentLineIndex);

        else if (operator instanceof LogicalFilter lf)
            currentLineIndex = encode(lf, builder, currentLineIndex);

        else if (operator instanceof LogicalJoin lj)
            currentLineIndex = encode(lj, builder, currentLineIndex);

        else if (operator instanceof LogicalProject lp)
            currentLineIndex = encode(lp, builder, currentLineIndex);

        else
            throw new UnsupportedOperationException("The current operator type cannot be encoded: " + operator.getClass());

        return currentLineIndex;
    }

    private static int encode(LogicalArrowTableScan scan, StringBuilder builder, int currentLineIndex) {
        // Output the scan node
        // Line form: S;{table name};{boolean indicating if columns are projected};{projected column indices separated by commas}\n
        builder.append("S;");

        RelOptTable table = scan.getTable();
        ArrowTable arrowTable = table.unwrap(ArrowTable.class);
        assert arrowTable != null;
        builder.append(arrowTable.getName());

        builder.append(';');

        ImmutableIntList projectedColumns = scan.projects;
        int projectedColumnsCount = projectedColumns.size();
        boolean isIdentity = table.getRowType().getFieldCount() == projectedColumnsCount;
        if (isIdentity)
            builder.append("false;");
        else
            builder.append("true;");

        for (int i = 0; i < projectedColumnsCount; i++) {
            builder.append(projectedColumns.get(i));

            if (i != projectedColumnsCount - 1)
                builder.append(',');
        }

        builder.append('\n');

        return currentLineIndex;
    }

    private static int encode(LogicalAggregate aggregate, StringBuilder builder, int currentLineIndex) {
        // Check pre-conditions
        if (aggregate.getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator: We expect exactly one GroupSet to exist in the logical plan");

        for (AggregateCall call : aggregate.getAggCallList())
            if (call.isDistinct())
                throw new UnsupportedOperationException(
                        "AggregationOperator does not support DISTINCT keyword");

        // First translate the input of the logical aggregate
        int lineIndexForInput = encode(aggregate.getInput(), builder, currentLineIndex);
        int lineIndexForAggregate = lineIndexForInput + 1;

        // Then output the aggregate node
        // Line form: A;{input node line index};{possible group-by column indides};{aggregation expressions separated by comma's}
        builder.append("A;");
        builder.append(lineIndexForInput);
        builder.append(';');

        ImmutableBitSet groupBySet = aggregate.getGroupSet();
        int numberOfGroupByKeys = groupBySet.cardinality();
        if (numberOfGroupByKeys > 0) {
            int[] groupByColumns = groupBySet.toArray();
            for (int i = 0; i < numberOfGroupByKeys; i++) {
                builder.append(groupByColumns[i]);

                if (i != numberOfGroupByKeys - 1)
                    builder.append(',');
            }
        }
        builder.append(';');

        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        int numberOfAggCalls = aggCalls.size();
        for (int i = 0; i < numberOfAggCalls; i++) {
            builder.append(aggCalls.get(i));

            if (i != numberOfAggCalls - 1)
                builder.append(',');
        }

        builder.append('\n');

        return lineIndexForAggregate;
    }

    private static int encode(LogicalFilter filter, StringBuilder builder, int currentLineIndex) {
        // First translate the input of the logical filter
        int lineIndexForInput = encode(filter.getInput(), builder, currentLineIndex);
        int lineIndexForFilter = lineIndexForInput + 1;

        // Expand the search operator
        RexNode filterCondition = filter.getCondition();
        RexNode expandedCondition = RexUtil.expandSearch(filter.getCluster().getRexBuilder(), null, filterCondition);

        // Then output the filter node
        // Line form: F;{input node line index};{condition}\n
        builder.append("F;");
        builder.append(lineIndexForInput);
        builder.append(';');
        builder.append(expandedCondition);
        builder.append('\n');

        return lineIndexForFilter;
    }

    private static int encode(LogicalJoin join, StringBuilder builder, int currentLineIndex) {
        // Check pre-conditions
        if (join.getJoinType() != JoinRelType.INNER)
            throw new UnsupportedOperationException("JoinOperator currently only supports inner joins");

        RexNode joinCondition = join.getCondition();
        if (!(joinCondition instanceof RexCall joinConditionCall))
            throw new UnsupportedOperationException("JoinOperator currently only supports join conditions wrapped in a RexCall");

        if (joinConditionCall.getOperator().getKind() != SqlKind.EQUALS)
            throw new UnsupportedOperationException("JoinOperator currently only supports equality join conditions");

        if (joinConditionCall.getOperands().size() != 2)
            throw new UnsupportedOperationException("JoinOperator currently only supports join conditions over two operands");

        List<RexNode> joinConditionOperands = joinConditionCall.getOperands();
        for (RexNode joinConditionOperand : joinConditionOperands) {
            if (!(joinConditionOperand instanceof RexInputRef))
                throw new UnsupportedOperationException("JoinOperator currently only supports join condition operands that refer to an input column");
        }

        if (joinConditionOperands.get(0).getType() != joinConditionOperands.get(1).getType())
            throw new UnsupportedOperationException("JoinOperator expects the join condition operands to be of the same type");

        // Join condition passes pre-condition, now re-order the predicate so it always has the
        // lhs join column's predicate first, followed by the rhs join column predicate
        RexInputRef firstRef = (RexInputRef) joinConditionOperands.get(0);
        RexInputRef secondRef = (RexInputRef) joinConditionOperands.get(1);
        if (firstRef.getIndex() > secondRef.getIndex()) {
            RexInputRef temp = secondRef;
            secondRef = firstRef;
            firstRef = temp;
        }

        // First translate the inputs of the logical join
        int lineIndexForLeftInput = encode(join.getLeft(), builder, currentLineIndex);
        int lineIndexForRightInput = encode(join.getRight(), builder, lineIndexForLeftInput + 1);
        int lineIndexForJoin = lineIndexForRightInput + 1;

        // Then output the join node
        // Line form: J;{left input node line index};{right input node line index};{left_column_eq_index};{right_column_eq_index}\n
        builder.append("J;");
        builder.append(lineIndexForLeftInput);
        builder.append(';');
        builder.append(lineIndexForRightInput);
        builder.append(';');
        builder.append(firstRef.getIndex());
        builder.append(";");
        builder.append(secondRef.getIndex());
        builder.append('\n');

        return lineIndexForJoin;
    }

    private static int encode(LogicalProject project, StringBuilder builder, int currentLineIndex) {
        // First translate the input of the logical project
        int lineIndexForInput = encode(project.getInput(), builder, currentLineIndex);
        int lineIndexForProject = lineIndexForInput + 1;

        // Then, output the project node
        // Line form: P;{input node line index};[{projection expressions}]\n
        builder.append("P;");
        builder.append(lineIndexForInput);
        builder.append(';');
        builder.append(project.getProjects());
        builder.append('\n');

        return lineIndexForProject;
    }

}
