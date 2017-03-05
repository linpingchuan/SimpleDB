package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field.
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field.
         * */
        public final Type fieldType;
        
        /**
         * The name of the field.
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final ArrayList<TDItem> tupleDItems;

    /**
     * @return An iterator which iterates over all the field tupleDItems that
     * are included in this TupleDesc.
     * */
    public Iterator<TDItem> iterator() {
        return tupleDItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr Array specifying the number of and types of fields in this
     * TupleDesc. It must contain at least one entry.
     *
     * @param fieldAr Array specifying the names of the fields. Note that names
     * may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tupleDItems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length && i < fieldAr.length; ++i) {
            tupleDItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr Array specifying the number of and types of fields in this
     * TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        tupleDItems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; ++i) {
            tupleDItems.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return The number of fields in this TupleDesc.
     */
    public int numFields() {
        return tupleDItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i Index of the field name to return. It must be a valid index.
     *
     * @return The name of the ith field.
     *
     * @throws NoSuchElementException If i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return tupleDItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i The index of the field to get the type of. It must be a valid
     * index.
     *
     * @return The type of the ith field.
     *
     * @throws NoSuchElementException If i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields()) {
            throw new NoSuchElementException();
        }
        return tupleDItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name Name of the field.
     *
     * @return The index of the field that is first to have the given name.
     *
     * @throws NoSuchElementException If no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tupleDItems.size(); ++i) {
            String fieldName = tupleDItems.get(i).fieldName;

            if (fieldName != null && fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int ret = 0;

        for (int i = 0; i < tupleDItems.size(); ++i) {
            ret += tupleDItems.get(i).fieldType.getLen();
        }
        return ret;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1 The TupleDesc with the first fields of the new TupleDesc.
     *
     * @param td2 The TupleDesc with the last fields of the TupleDesc.
     *
     * @return The new TupleDesc.
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int numFields = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];

        for (int i = 0; i < td1.numFields(); ++i) {
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }
        for (int i = td1.numFields(); i < numFields; ++i) {
            typeAr[i] = td2.getFieldType(i - td1.numFields());
            fieldAr[i] = td2.getFieldName(i - td1.numFields());
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o The Object to be compared for equality with this TupleDesc.
     *
     * @return True if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
            TupleDesc t = (TupleDesc) o;
            if (t.numFields() != numFields()) {
                return false;
            }
            for (int i = 0; i < numFields(); ++i) {
                if (!getFieldType(i).equals(t.getFieldType(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String ret = new String();

        for (int i = 0; i < numFields(); ++i) {
            ret += getFieldType(i) + "(" + getFieldName(i) + ")";
            if (i + 1 != numFields()) {
                ret += ", ";
            }
        }
        return ret;
    }
}
