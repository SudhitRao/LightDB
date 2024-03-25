package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SumOperator extends Operator {

    private Operator child;

    private Map<ArrayList<Integer>, ArrayList<Integer>> seenMap = new HashMap<>();

    private List<Tuple> buffer = new ArrayList<>();

    private int index = 0;


    public SumOperator(Operator child, List<String> groupByColumns, List<String> productColumns, String sumName) {
        this.child = child;
        Tuple curr;
        tupleSchema = new ArrayList<>();
        for (String col : groupByColumns) {
            tupleSchema.add(col);
        }
        for (String col : child.tupleSchema) {
            if (tupleSchema.indexOf(col) == -1) {
                tupleSchema.add(col);
            }
        }
        if (productColumns != null) {
            tupleSchema.add(sumName);
        }
        while ((curr = child.getNextTuple()) != null) {
            ArrayList<Integer> keyList = new ArrayList<>();
            for (String col : groupByColumns) {
                keyList.add(curr.get(child.tupleSchema.indexOf(col))); 
            }
            ArrayList<Integer> list;
            if (seenMap.containsKey(keyList)) {
                list = seenMap.get(keyList);
                int idx = 0;
                for (String col : child.tupleSchema) {
                    if (groupByColumns.indexOf(col) == -1) {
                        list.set(idx, list.get(idx) + curr.get(child.tupleSchema.indexOf(col)));
                        idx++;
                    }
                }
                if (productColumns != null) {
                    int agg = 1;
                    for (String s : productColumns) {
                        if (child.tupleSchema.indexOf(s) == -1) {
                            agg *= Integer.valueOf(s);
                        } else {
                            agg *= curr.get(child.tupleSchema.indexOf(s));
                        }
                    }
                    list.set(idx, list.get(idx) + agg);
                }

            } else {
                list = new ArrayList<>();
                for (String col : child.tupleSchema) {
                    if (groupByColumns.indexOf(col) == -1) {
                        list.add(curr.get(child.tupleSchema.indexOf(col)));
                    }
                }
                if (productColumns != null) {
                    int agg = 1;
                    for (String s : productColumns) {
                        if (child.tupleSchema.indexOf(s) == -1) {
                            agg *= Integer.valueOf(s);
                        } else {
                            agg *= curr.get(child.tupleSchema.indexOf(s));
                        }
                    }
                    list.add(agg);
                }
                seenMap.put(keyList, list);
            }
        }

        for (Entry<ArrayList<Integer>, ArrayList<Integer>> entry : seenMap.entrySet()) {
            ArrayList<Integer> tupleList = new ArrayList<>();
            tupleList.addAll(entry.getKey());
            tupleList.addAll(entry.getValue());
            buffer.add(new Tuple(tupleList));
        }
        //System.out.println(seenMap);
        //System.out.println(tupleSchema);
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
