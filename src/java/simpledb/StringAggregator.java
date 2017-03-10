package simpledb;

import java.util.*;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private final int gbfield;
  private final Type gbfieldtype;
  private final int afield;

  // Op is always COUNT.

  private final HashMap<Field, Integer> ares;

  /**
   * Aggregate constructor.
   *
   * @param gbfield The 0-based index of the group-by field in the tuple, or
   * NO_GROUPING if there is no grouping.
   *
   * @param gbfieldtype The type of the group by field (e.g., Type.INT_TYPE),
   * or null if there is no grouping.
   *
   * @param afield The 0-based index of the aggregate field in the tuple.
   *
   * @param op Aggregation operator to use (only supports COUNT).
   *
   * @throws IllegalArgumentException If op != COUNT.
   */
  public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op op) {
    if (op != Op.COUNT) {
      throw new IllegalStateException();
    }

    this.gbfield = gbfield;
    this.gbfieldtype = gbfieldtype;
    this.afield = afield; // Is it useful?

    this.ares = new HashMap<Field, Integer>();
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the
   * constructor.
   *
   * @param tup The Tuple containing an aggregate field and a group-by field.
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    Field gbv = gbfield != NO_GROUPING ? tup.getField(gbfield) : null;
    Integer v = ares.get(gbv);

    ares.put(gbv, v == null ? 1 : v + 1);
  }

  /**
   * Create a DbIterator over group aggregate results.
   *
   * @return A DbIterator whose tuples are the pair (groupVal, aggregateVal) if
   * using group, or a single (aggregateVal) if no grouping. The aggregateVal
   * is determined by the type of aggregate specified in the constructor.
   */
  public DbIterator iterator() {
    TupleDesc td;
    ArrayList<Tuple> tuples = new ArrayList<Tuple>();

    if (gbfield != NO_GROUPING) {
      td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});

      for (Entry<Field, Integer> e : ares.entrySet()) {
        Tuple tuple = new Tuple(td);

        tuple.setField(0, e.getKey());
        tuple.setField(1, new IntField(e.getValue()));

        tuples.add(tuple);
      }
    } else {
      td = new TupleDesc(new Type[] {Type.INT_TYPE});

      // TODO(foreverbell): Should we return 0 if the db is empty?
      if (!ares.isEmpty()) {
        Tuple tuple = new Tuple(td);

        tuple.setField(0, new IntField(ares.get(null)));

        tuples.add(tuple);
      }
    }

    return new TupleIterator(td, tuples);
  }

}
