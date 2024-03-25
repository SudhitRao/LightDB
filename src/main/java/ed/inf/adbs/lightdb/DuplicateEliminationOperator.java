package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

// Distinct Operator (hashing based implementation)

public class DuplicateEliminationOperator extends Operator {

    private Operator child;

    Set<Tuple> seen = new HashSet<>();

    /*
     * constructor
     * @param child child operator
     */
    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
        tupleSchema = (ArrayList<String>) child.tupleSchema.clone();
    }


    /*
     * get the next tuple
     * @return next tuple value
     */
    @Override
    public Tuple getNextTuple() {
        Tuple curr = child.getNextTuple();
        while (curr != null && seen.contains(curr)) {
            curr = child.getNextTuple();
        }
        if (curr == null) {
            return null;
        }
        seen.add(curr);
        return curr;
    }

    /*
     * reset the operator
     */
    @Override
    public void reset() {
        seen.clear();
        child.reset();
    }
    
}
