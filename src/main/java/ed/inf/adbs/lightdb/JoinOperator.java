package ed.inf.adbs.lightdb;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator {

    private Operator leftChild;
    private Operator rightChild;

    private Tuple leftTuple;

    private Expression whereCondition;


    public JoinOperator(Operator leftChild, Operator rightChild, Expression whereCondition) {
        tupleSchema = new ArrayList<>();
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.whereCondition = whereCondition;
        leftTuple = null;
        for (String s : leftChild.tupleSchema) {
            tupleSchema.add(s);
        }
        for (String s : rightChild.tupleSchema) {
            tupleSchema.add(s);
        }

    }
    
    @Override
    public Tuple getNextTuple() {
        if (leftTuple == null) {
            leftTuple = leftChild.getNextTuple();
        }
        while (leftTuple != null) {
            Tuple rightTuple;
            while ((rightTuple = rightChild.getNextTuple()) != null) {
                if (whereCondition == null) {
                    Tuple toReturn = Tuple.merge(leftTuple, rightTuple);
                    return toReturn;
                } else {
                    TupleExpressionEvaluator evaluator = new TupleExpressionEvaluator(leftTuple, 
                    rightTuple, leftChild.tupleSchema, rightChild.tupleSchema);
                    whereCondition.accept(evaluator);
                    if (evaluator.getResult()) {
                        return Tuple.merge(leftTuple, rightTuple);
                    }
                }
            }
            leftTuple = leftChild.getNextTuple();
            rightChild.reset();
        }
        return null;
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        leftTuple = null;
    }
    
}
