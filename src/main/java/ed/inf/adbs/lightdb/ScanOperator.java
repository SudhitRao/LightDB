package ed.inf.adbs.lightdb;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/*
 * ScanOperator class. Simply takes in the table and returns the next tuple on by one by reading the file. 
 */

public class ScanOperator extends Operator {
    private DataCatalog catalog;
    private BufferedReader reader;
    private String currentLine;
    private String table;


    /*
     * Constructor, creates a scan operator for a table
     * @param table table name
     * @param alias alias/table name if no alias
     */
    public ScanOperator(String table, String alias) {
        tupleSchema = new ArrayList<>();
        this.catalog = DataCatalog.getInstance();
        this.table = table;
        try {
            reader = new BufferedReader(new FileReader(catalog.getPath(table)));
        } catch (IOException e) {
            System.err.println("Could not open file: " + catalog.getPath(table));
            e.printStackTrace();
        }
        for (String col : catalog.getSchema(table)) {
            tupleSchema.add(alias + "." + col);
        }
        
    }

    /*
     * get the next tuple
     * @return next tuple
     */
    @Override
    public Tuple getNextTuple() {
        try {
            // Read the next line from the file
            currentLine = reader.readLine();

            // If the end of the file has been reached, return null
            if (currentLine == null) {
                return null;
            }
            //System.out.println(currentLine.replaceAll("\\s", ""));
            String[] arr = currentLine.replaceAll("\\s", "").split(",");
            
            return new Tuple(arr);
        } catch (IOException e) {
            System.err.println("Error reading next line from file.");
            e.printStackTrace();
            return null;
        }
    }

    /*
     * reset the operator
     */
    @Override
    public void reset() {
        try {
            reader = new BufferedReader(new FileReader(catalog.getPath(table)));
        } catch (IOException e) {
            System.err.println("Could not open file: " + catalog.getPath(table));
            e.printStackTrace();
        }
    }
}
