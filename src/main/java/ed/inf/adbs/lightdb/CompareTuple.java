package ed.inf.adbs.lightdb;

import java.util.Comparator;

public class CompareTuple implements Comparator<Tuple> {

    private int[] indexes;

    public CompareTuple(int[] indexes) {
        this.indexes = indexes;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        
        for (int i = 0; i < indexes.length; i++) {
            int val1 = t1.get(indexes[i]);
            int val2 = t2.get(indexes[i]);
            if (val1 < val2) {
                return -1;
            } else if (val1 > val2) {
                return 1;
            }
        }
        return 0;
        }
    }


