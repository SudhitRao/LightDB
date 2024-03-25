package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DataCatalog {

    private static DataCatalog instance;

    private ArrayList<String> tableList;

    private Map<String, ArrayList<String>> schemaMap;

    private Map<String, String> pathMap;



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
    
    public static void initialize(String dirString) {
        if (instance == null) {
            instance = new DataCatalog(dirString);
        } else {
            throw new IllegalStateException("Instance already initialized");
        }
    }

    public static DataCatalog getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Instance has not been initialized");
        } else {
            return instance;
        }
    }
    
    // get schema for table 
    public ArrayList<String> getSchema(String table) {
        return schemaMap.get(table);
    }

    public ArrayList<String> getTableList() {
        return tableList;
    }

    // get the filepath to the csv containing the values of table
    public String getPath(String table) {
        return pathMap.get(table);
    }

    public int getColumnIndex(String table, String column) {
        //System.out.println(schemaMap.get(table));
        return schemaMap.get(table).indexOf(column);
    }
    
}
