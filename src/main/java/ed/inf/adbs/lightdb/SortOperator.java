package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;


public class SortOperator extends Operator {

    private List<Tuple> buffer = new ArrayList<>();

    private int index = 0;

    /*
     * Constructor. Call getnexttuple on the child operator and store it in buffer. Then sort. 
     * @param child child operator of sort
     * @param orderList elements to orderby in order
     */
    public SortOperator(Operator child, List<OrderByElement> orderList) {
        tupleSchema = (ArrayList<String>) child.tupleSchema.clone();
        int[] indexes = new int[orderList.size()];
        for (int i = 0; i < orderList.size(); i++) {
            indexes[i] = tupleSchema.indexOf(orderList.get(i).toString());
        }
        Tuple nextTuple;
        while ((nextTuple = child.getNextTuple()) != null) {
            buffer.add(nextTuple);
        }
        CompareTuple tupleCompare = new CompareTuple(indexes);
        Collections.sort(buffer, tupleCompare);
    }

    /*
     * get the next tuple from the buffer
     * @return next tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (index >= buffer.size()) {
            return null;
        }
        return buffer.get(index++);
    }  

    /*
     * reset the operator (just index = 0)
     */
    @Override
    public void reset() {
        index = 0;
    }
}
