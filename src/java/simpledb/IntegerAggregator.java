package simpledb;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

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
   * @param what The aggregation operator.
   */

  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    // some code goes here
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the
   * constructor.
   * 
   * @param tup The Tuple containing an aggregate field and a group-by field.
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    // some code goes here
  }

  /**
   * Create a DbIterator over group aggregate results.
   * 
   * @return A DbIterator whose tuples are the pair (groupVal, aggregateVal)
   * if using group, or a single (aggregateVal) if no grouping. The aggregateVal
   * is determined by the type of aggregate specified in the constructor.
   */
  public DbIterator iterator() {
    // some code goes here
    throw new UnsupportedOperationException("please implement me for lab2");
  }

}
