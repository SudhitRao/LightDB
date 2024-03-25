package ed.inf.adbs.lightdb;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;;

public class SelectOperator extends Operator {

    private Operator child;
    private Expression whereCondition;

    /*
     * constructor to create a selection operator
     * @param childOperator child of selection operator
     * @param whereCondition Expression to evaluate selection on incoming tuples
     */
    public SelectOperator(Operator childOperator, Expression whereCondition) {
        tupleSchema = new ArrayList<>();
        this.child = childOperator;
        this.whereCondition = whereCondition;
        for (String s : child.tupleSchema) {
            tupleSchema.add(s);
        }
    }

    /*
     * get the next tuple only if it satisfies the selection condition
     * @return next tuple
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            TupleExpressionEvaluator evaluator = new TupleExpressionEvaluator(tuple, child.getTupleSchema());
            whereCondition.accept(evaluator);
            if (evaluator.getResult()) {
                return tuple;
            }
        }
        return null;
    }

    /*
     * reset the operator
     */
    @Override
    public void reset() {
       child.reset();
    }
}
