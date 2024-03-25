package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator extends Operator {

    private Operator child;
    private int[] indexes;

    public ProjectOperator(Operator child, List<SelectItem<?>> cols) {
        tupleSchema = new ArrayList<>();
        this.child = child;
        this.indexes = new int[cols.size()];
        int i = 0;
        for (SelectItem<?> item : cols) {
            Expression exp = item.getExpression();
            if (exp instanceof Column) {
                Column col = (Column) exp;
                String tableName = col.getTable().getName();
                String colName = col.getColumnName();
                indexes[i] = child.tupleSchema.indexOf(tableName + "." + colName);
                tupleSchema.add(tableName + "." + colName);
            } else {
                indexes[i] = child.tupleSchema.indexOf(exp.toString());
                tupleSchema.add(exp.toString());
            }
            i++;
        }
    }

    public ProjectOperator(Operator child, List<String> cols, boolean tmp) {
        tupleSchema = new ArrayList<>();
        this.child = child;
        this.indexes = new int[cols.size()];
        int i = 0;
        for (String item : cols) {
            tupleSchema.add(item);
            indexes[i] = child.tupleSchema.indexOf(item);
            i++;
        }
        
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tup = child.getNextTuple();
        if (tup != null) {
            ArrayList<Integer> extracted = new ArrayList<>();
            for (int index : indexes) {
                extracted.add(tup.get(index));
            }
            return new Tuple(extracted);
        } 
        return null;        
    }

    @Override
    public void reset() {
        child.reset();
    }
    
}
