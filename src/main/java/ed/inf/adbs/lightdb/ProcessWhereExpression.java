package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessWhereExpression extends ExpressionDeParser {
    private Map<String, List<Expression>> joinConditions = new HashMap<>();
    private Map<String, List<Expression>> selectionConditions = new HashMap<>();

    private void processComparison(Expression left, Expression right, ComparisonOperator op) {
        
        Column leftColumn = null;
        Column rightColumn = null;

        if (left instanceof Column) {
            leftColumn = (Column) left;
        }
        
        if (right instanceof Column) {
            rightColumn = (Column) right;
        }

        // Determine if this is a join condition or a selection condition
        if (leftColumn != null && rightColumn != null) {
            // This is considered a join condition if both sides are columns potentially from different tables
            String leftTableName = leftColumn.getTable().getName();
            String rightTableName = rightColumn.getTable().getName();

            if (!leftTableName.equals(rightTableName)) {
                joinConditions.putIfAbsent(leftTableName + "_" + rightTableName, new ArrayList<>());
                joinConditions.get(leftTableName + "_" + rightTableName).add(op);
                joinConditions.putIfAbsent(rightTableName + "_" + leftTableName, new ArrayList<>());
                joinConditions.get(rightTableName + "_" + leftTableName).add(op);
            } else {
                // Otherwise, it's a selection condition
                addSelectionCondition(leftTableName, op);
            }
        } else if (leftColumn != null) {
            addSelectionCondition(leftColumn.getTable().getName(), op);
        } else if (rightColumn != null) {
            addSelectionCondition(rightColumn.getTable().getName(), op);
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        processComparison(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), equalsTo);
    }

    @Override
    public void visit(MinorThan minorThan) {
        processComparison(minorThan.getLeftExpression(), minorThan.getRightExpression(), minorThan);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        processComparison(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression(), notEqualsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        processComparison(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        processComparison(greaterThanEquals.getLeftExpression(), greaterThanEquals.getRightExpression(), greaterThanEquals);
    }


    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        processComparison(minorThanEquals.getLeftExpression(), minorThanEquals.getRightExpression(), minorThanEquals);
    }


    private void addSelectionCondition(String tableName, Expression condition) {
        selectionConditions.putIfAbsent(tableName, new ArrayList<>());
        selectionConditions.get(tableName).add(condition);
    }

    public Map<String, List<Expression>> getJoinConditions() {
        return joinConditions;
    }

    public Map<String, List<Expression>> getSelectionConditions() {
        return selectionConditions;
    }
}
