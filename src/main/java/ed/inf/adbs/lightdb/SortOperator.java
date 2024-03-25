package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;


public class SortOperator extends Operator {

    private List<Tuple> buffer = new ArrayList<>();

    private int index = 0;

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

    @Override
    public Tuple getNextTuple() {
        if (index >= buffer.size()) {
            return null;
        }
        return buffer.get(index++);
    }

    @Override
    public void reset() {
        index = 0;
    }
}
