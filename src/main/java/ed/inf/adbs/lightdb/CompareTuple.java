package ed.inf.adbs.lightdb;

import java.util.Comparator;

// Comparater class to find tuple order for the sort operator

public class CompareTuple implements Comparator<Tuple> {

    private int[] indexes;

    /*
     * Constructor
     * @param indexes priority of indexes
     */

    public CompareTuple(int[] indexes) {
        this.indexes = indexes;
    }

    /*
     * compare two tuples method
     * @param t1 first tuple
     * @param t2 second tuple
     * @return -1 if t1 < t2, 1 if t1 > t2, else 0
     */
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


