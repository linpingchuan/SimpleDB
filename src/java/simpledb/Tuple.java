package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

  private static final long serialVersionUID = 1L;

  private TupleDesc td = null;
  private ArrayList<Field> fields = null;
  private RecordId recordId = null;

  /**
   * Create a new tuple with the specified schema (type).
   * 
   * @param td The schema of this tuple. It must be a valid TupleDesc instance
   * with at least one field.
   */
  public Tuple(TupleDesc td) {
    resetTupleDesc(td);
  }

  /**
   * Returns the TupleDesc representing the schema of this tuple.
   */
  public TupleDesc getTupleDesc() {
    return td;
  }

  /**
   * Returns the RecordId representing the location of this tuple on disk.
   * May be null.
   */
  public RecordId getRecordId() {
    return recordId;
  }

  /**
   * Set the RecordId information for this tuple.
   * 
   * @param recordId The new RecordId for this tuple.
   */
  public void setRecordId(RecordId recordId) {
    this.recordId = recordId;
  }

  /**
   * Change the value of the ith field of this tuple.
   * 
   * @param i Index of the field to change. It must be a valid index.
   *
   * @param f New value for the field.
   */
  public void setField(int i, Field f) {
    fields.set(i, f);
  }

  /**
   * @return The value of the ith field, or null if it has not been set.
   * 
   * @param i Field index to return. Must be a valid index.
   */
  public Field getField(int i) {
    return fields.get(i);
  }

  /**
   * Returns the contents of this Tuple as a string. Note that to pass the
   * system tests, the format needs to be as follows:
   * 
   * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
   * 
   * where \t is any whitespace, except newline, and \n is a newline.
   */
  public String toString() {
    String ret = new String();
    for (int i = 0; i < fields.size(); ++i) {
      ret += fields.get(i).toString();
      ret += (i + 1 == fields.size() ? "\n" : " ");
    }
    return ret;
  }
  
  /**
   * Returns an iterator which iterates over all the fields of this tuple.
   * */
  public Iterator<Field> fields() {
    return fields.iterator();
  }
  
  /**
   * Reset the TupleDesc of this tuple.
   * */
  public void resetTupleDesc(TupleDesc td) {
    this.td = td;

    this.fields = new ArrayList<Field>(td.numFields());
    for (int i = 0; i < td.numFields(); ++i) {
      this.fields.add(null);
    }
  }
}
