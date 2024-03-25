package ed.inf.adbs.lightdb;

import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

// Interpreter class, that takes in the input file, output file, database directory, and executes query plan

public class Interpreter {

    private Statement statement;
    private String outputFile;
    private boolean fromFile;
    

    /*
     * Constructor
     * @param dataBaseDir data directory
     * @param input path to file containing query or query itself
     * @param outputFile output path
     * @param fromFile whether input is path or query
     */
    public Interpreter(String databaseDir, String input, String outputFile, boolean fromFile) {
        this.fromFile = fromFile;
        if (fromFile) {
            try {
                statement = CCJSqlParserUtil.parse(new FileReader(input));
            } catch (Exception e) {
                System.err.println("Exception occurred during parsing");
                e.printStackTrace();
            }
            this.outputFile = outputFile;
            DataCatalog.initialize(databaseDir);
        } else {
            try {
                statement = CCJSqlParserUtil.parse(input);
            } catch (Exception e) {
                System.err.println("Exception occurred during parsing");
                e.printStackTrace();
            }
            this.outputFile = outputFile;
            DataCatalog.initialize(databaseDir);
        }
        
    }

    /*
     * Build query plan and dump the root operator into file.
     */
    public void execute() throws Exception {
        QueryPlanBuilder builder = new QueryPlanBuilder();
        Operator root = builder.buildQueryPlan(statement);
        if (fromFile) {
            root.dump(outputFile);
        } else {
            root.dump();
        }
       
    }






    
}
