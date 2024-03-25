package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

/*
 * Projection operator. Simply finds the indexes to keep and returns them when sending the next tuple 
 * to the parent or output. 
 */
public class ProjectOperator extends Operator {

    private Operator child;
    private int[] indexes;

    /*
     * Constructor. Finds all indexes in the tuple to include and which ones to eliminate
     * @param child child operator
     * @param cols list of columns to project
     */
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

    /*
     * Constructor. Finds all indexes in the tuple to include and which ones to eliminate
     * @param child child operator
     * @param cols list of columns to project
     */
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


    /*
     * get the next tuple
     * @return the next tuple to return after joining and merging the left and right child
     */
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

    /*
     * reset the operator.
     */
    @Override
    public void reset() {
        child.reset();
    }
    
}
