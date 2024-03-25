package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class DuplicateEliminationOperator extends Operator {

    private Operator child;

    Set<Tuple> seen = new HashSet<>();

    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
        tupleSchema = (ArrayList<String>) child.tupleSchema.clone();
    }

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

    @Override
    public void reset() {
        seen.clear();
        child.reset();
    }
    
}
