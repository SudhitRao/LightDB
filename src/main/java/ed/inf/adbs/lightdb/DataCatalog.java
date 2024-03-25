package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

// Data catalog class to hold informatin about the different tables and the schema and file locations

public class DataCatalog {

    private static DataCatalog instance;

    private ArrayList<String> tableList;

    private Map<String, ArrayList<String>> schemaMap;

    private Map<String, String> pathMap;


    /*
     * Constructor
     * @param dataDir path to data directory
     */
    public DataCatalog(String dataDir) {

        BufferedReader reader = null;
        String filePath = dataDir + "/schema.txt";
        tableList = new ArrayList<>();
        schemaMap = new HashMap<>();
        pathMap = new HashMap<>();

        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;

            while ((line = reader.readLine()) != null) {
                //System.out.println(line);
                String[] tmp = line.split(" ");
                tableList.add(tmp[0]);
                ArrayList<String> rest = new ArrayList<>();
                for (int i  = 1; i < tmp.length; i++) {
                    rest.add(tmp[i]);
                }
                schemaMap.put(tmp[0], rest);
                pathMap.put(tmp[0],  dataDir + "/data/" + tmp[0] + ".csv");
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    System.err.println("Error closing the file: " + e.getMessage());
                }
            }
        }
    }

    /*
     * Initialize using constructor
     * @param dirString path to data directory
     */
    public static void initialize(String dirString) {
        if (instance == null) {
            instance = new DataCatalog(dirString);
        } else {
            throw new IllegalStateException("Instance already initialized");
        }
    }


    /*
     * get the singleton instance
     * @return returns the instance
     */
    public static DataCatalog getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Instance has not been initialized");
        } else {
            return instance;
        }
    }
    
    /*
     * get schema for table
     * @param table name of table
     * @return list of table names
     */
    public ArrayList<String> getSchema(String table) {
        return schemaMap.get(table);
    }

    /*
     * get table names
     * @return list of table names
     */
    public ArrayList<String> getTableList() {
        return tableList;
    }

    /*
     * get the filepath to the csv containing the values of table
     * @return get filepath of table
     */
    public String getPath(String table) {
        return pathMap.get(table);
    }

    /*
     * get column index
     * @param table tbale name
     * @param column column name
     * @return index in csv file
     */
    public int getColumnIndex(String table, String column) {
        return schemaMap.get(table).indexOf(column);
    }
    
}
