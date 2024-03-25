package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;

/*
 * Join Operator implementation.
 * Given the conditions to join with (or null) left and right child are joined. 
 * Uses the simple nested loop algorithm to iterate through the left child and right child
 */
public class JoinOperator extends Operator {

    private Operator leftChild;
    private Operator rightChild;

    private Tuple leftTuple;

    private List<Expression> whereConditions;

    /*
     * Constuctor
     * @param leftChild left operator
     * @param rightChild right operator
     * @param whereConditions the conditions to join the left and right operator
     */
    public JoinOperator(Operator leftChild, Operator rightChild, List<Expression> whereConditions) {
        tupleSchema = new ArrayList<>();
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.whereConditions = whereConditions;
        leftTuple = null;
        for (String s : leftChild.tupleSchema) {
            tupleSchema.add(s);
        }
        for (String s : rightChild.tupleSchema) {
            tupleSchema.add(s);
        }

    }
    
    /*
     * get the next tuple
     * @return the next tuple to return after joining and merging the left and right child
     */
    @Override
    public Tuple getNextTuple() {
        if (leftTuple == null) {
            leftTuple = leftChild.getNextTuple();
        }
        while (leftTuple != null) {
            Tuple rightTuple;
            while ((rightTuple = rightChild.getNextTuple()) != null) {
                if (whereConditions.size() == 0) {
                    Tuple toReturn = Tuple.merge(leftTuple, rightTuple);
                    return toReturn;
                } else {
                    TupleExpressionEvaluator evaluator = new TupleExpressionEvaluator(leftTuple, 
                    rightTuple, leftChild.tupleSchema, rightChild.tupleSchema);
                    boolean accept = true;
                    for (int i = 0; i < whereConditions.size(); i++) {
                        if (whereConditions.get(i) == null) continue;
                        whereConditions.get(i).accept(evaluator);
                        if (!evaluator.getResult()) accept = false;
                    }
                    if (accept) {
                        return Tuple.merge(leftTuple, rightTuple);
                    }
                }
            }
            leftTuple = leftChild.getNextTuple();
            rightChild.reset();
        }
        return null;
    }


    /*
     * reset the operator.
     */
    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        leftTuple = null;
    }
    
}
