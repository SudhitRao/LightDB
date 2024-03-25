package ed.inf.adbs.lightdb;

import java.util.ArrayList;

public class Tuple {
    private ArrayList<Integer> fields;
    private int len;

    public Tuple(String[] param) {
        this.len = param.length;
        fields = new ArrayList<>();

        for (String s : param) {
            fields.add(Integer.parseInt(s));
        }
    }

    public Tuple(ArrayList<Integer> param) {
        this.len = param.size();
        this.fields = param;
    }
    
    public static Tuple merge(Tuple og, Tuple other) {
        ArrayList<Integer> arr = new ArrayList<>();
        for (int i = 0; i < og.getLength(); i++) {
            arr.add(og.get(i)); 
        }
        Tuple toReturn = new Tuple(arr);
        for (int i = 0; i < other.getLength(); i++) {
            toReturn.fields.add(other.get(i));
        }
        return toReturn;
    }

    @Override
    public String toString() {
        return fields.toString();
    }

    public int get(int index) {
        return fields.get(index);
    }

    public int getLength() {
        return fields.size();
    }

    public ArrayList<Integer> getArray() {
        return fields;
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Tuple)) {
            return false;
        }
        Tuple tmp = (Tuple) other;
        return this.hashCode() == tmp.hashCode();
    }
}