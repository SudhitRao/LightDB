package ed.inf.adbs.lightdb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


public abstract class Operator {

    protected ArrayList<String> tupleSchema;
    
    public abstract Tuple getNextTuple();

    public abstract void reset();

    public void dump() {
        reset(); 
        // System.out.println("starting");

        Tuple tuple = getNextTuple();
        while (tuple != null) {
            System.out.println(tuple);
            tuple = getNextTuple();
        }
    }

    public void dump(String filename) {
        reset();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            Tuple next;
            while ((next = getNextTuple()) != null) {
                String toPrint = next.toString();
                writer.write(toPrint.substring(1, toPrint.length() - 1));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    public ArrayList<String> getTupleSchema() {
        return tupleSchema;
    }
}