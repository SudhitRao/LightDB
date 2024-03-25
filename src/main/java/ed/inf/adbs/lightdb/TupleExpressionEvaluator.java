package ed.inf.adbs.lightdb;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/*
 * Tuple Expression Evaluator visitor class. Siumply takes two tuples. 
 * Given expression returns true or false if satisfies it or not. Or a single tuple if the
 * expression is not a join condition. 
 */
public class TupleExpressionEvaluator extends ExpressionDeParser {
    private Tuple leftTuple;
    private Tuple rightTuple;

    private boolean result;
    
    private ArrayList<String> leftSchema;
    private ArrayList<String> rightSchema;

    /*
     * Case where its just a selection condition, so only one tuple is provided
     * @param leftTuple the singular tuple to be evaluated
     * @param leftSchema the schema mapping of columns to tuple indices
     */
    public TupleExpressionEvaluator(Tuple leftTuple, ArrayList<String> leftSchema) {
        this.leftTuple = leftTuple;
        this.rightTuple = null;
        this.leftSchema = leftSchema;
        this.rightSchema = null;
    }
    /*
     * Case where its a join condition, so two tuples are provided
     * @param leftTuple the left tuple to be evaluated
     * @param rightTuple the right tuple to be evaluated
     * @param leftSchema the schema mapping of columns to tuple indices for left tuple
     * @param rightSchema the schema mapping of columns to tuple indices for right tuple
     */
    public TupleExpressionEvaluator(Tuple leftTuple,Tuple rightTuple, ArrayList<String> leftSchema, 
        ArrayList<String> rightSchema) {
            this.leftTuple = leftTuple;
            this.rightTuple = rightTuple;
            this.leftSchema = leftSchema;
            this.rightSchema = rightSchema;
    }

    private int parseIntValue(Expression expr) {
        if (expr instanceof LongValue) {
            return (int)((LongValue) expr).getValue();
        } else if (expr instanceof Column) {
            String colName = ((Column) expr).getColumnName();
            String tabName = ((Column) expr).getTable().getName();
            int leftIndex = leftSchema.indexOf(tabName + "." + colName);
            int rightIndex = -1;
            if (rightSchema != null) {
                rightIndex = rightSchema.indexOf(tabName + "." + colName);
            }
            if (leftIndex != -1) {
                return leftTuple.get(leftIndex);
            } else {
                return rightTuple.get(rightIndex);
            }
        }
        throw new IllegalArgumentException("Unsupported expression for integer value: " + expr);
    }

    private void processComparison(Expression left, Expression right, String operator) {
        int leftValue = parseIntValue(left);
        int rightValue = parseIntValue(right);

        switch (operator) {
            case "=":
                this.result = leftValue == rightValue;
                break;
            case "!=":
                this.result = leftValue != rightValue;
                break;
            case ">":
                this.result = leftValue > rightValue;
                break;
            case ">=":
                this.result = leftValue >= rightValue;
                break;
            case "<":
                this.result = leftValue < rightValue;
                break;
            case "<=":
                this.result = leftValue <= rightValue;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        processComparison(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), "=");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        processComparison(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression(), "!=");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        processComparison(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), ">");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        processComparison(greaterThanEquals.getLeftExpression(), greaterThanEquals.getRightExpression(), ">=");
    }

    @Override
    public void visit(MinorThan minorThan) {
        processComparison(minorThan.getLeftExpression(), minorThan.getRightExpression(), "<");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        processComparison(minorThanEquals.getLeftExpression(), minorThanEquals.getRightExpression(), "<=");
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftResult = this.result;

        andExpression.getRightExpression().accept(this);
        this.result = leftResult && this.result; // Combine with AND logic
    }

    public boolean getResult() {
        return result;
    }
}