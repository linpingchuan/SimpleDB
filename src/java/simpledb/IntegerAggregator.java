package simpledb;

import java.util.*;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private final int gbfield;
  private final Type gbfieldtype;
  private final int afield;
  private final Op op;

  private class Aggregated {
    public int v;
    public int c;
  };

  private final HashMap<Field, Aggregated> ares; // Aggregated result

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
   * @param op The aggregation operator.
   */
  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op op) {
    this.gbfield = gbfield;
    this.gbfieldtype = gbfieldtype;
    this.afield = afield;
    this.op = op;

    this.ares = new HashMap<Field, Aggregated>();
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the
   * constructor.
   * 
   * @param tup The Tuple containing an aggregate field and a group-by field.
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    Field gbv = gbfield != NO_GROUPING ? tup.getField(gbfield) : null;
    int av = ((IntField) tup.getField(afield)).getValue();

    Aggregated v = ares.get(gbv);

    if (v == null) {
      v = new Aggregated();
      v.v = av;
      v.c = 1;
    } else {
      switch (op) {
        case SUM:
        case AVG:
        case COUNT:
          v.v += av;
          break;
        case MIN:
          if (av < v.v) v.v = av;
          break;
        case MAX:
          if (av > v.v) v.v = av;
          break;
        default:
          throw new IllegalStateException("not implemented.");
      }
      v.c += 1;
    }

    ares.put(gbv, v);
  }

  public int extractValue(Aggregated a) {
    switch (op) {
      case SUM:
      case MIN:
      case MAX:
        return a.v;
      case AVG:
        return a.c == 0 ? 0 : a.v / a.c;
      case COUNT:
        return a.c;
      default:
        throw new IllegalStateException("not implemented.");
    }
  }

  /**
   * Create a DbIterator over group aggregate results.
   * 
   * @return A DbIterator whose tuples are the pair (groupVal, aggregateVal)
   * if using group, or a single (aggregateVal) if no grouping. The aggregateVal
   * is determined by the type of aggregate specified in the constructor.
   */
  public DbIterator iterator() {
    TupleDesc td;
    ArrayList<Tuple> tuples = new ArrayList<Tuple>();

    if (gbfield != NO_GROUPING) {
      td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});

      for (Entry<Field, Aggregated> e : ares.entrySet()) {
        Tuple tuple = new Tuple(td);

        tuple.setField(0, e.getKey());
        tuple.setField(1, new IntField(extractValue(e.getValue())));

        tuples.add(tuple);
      }
    } else {
      td = new TupleDesc(new Type[] {Type.INT_TYPE});

      if (!ares.isEmpty()) {
        Tuple tuple = new Tuple(td);

        tuple.setField(0, new IntField(extractValue(ares.get(null))));

        tuples.add(tuple);
      }
    }

    return new TupleIterator(td, tuples);
  }

}
