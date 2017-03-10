package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

  private static final long serialVersionUID = 1L;

  private DbIterator child;
  private final TupleDesc td;
  private final Predicate p;

  /**
   * Constructor accepts a predicate to apply and a child operator to read
   * tuples to filter from.
   * 
   * @param p The predicate to filter tuples with.
   *
   * @param child The child operator.
   */
  public Filter(Predicate p, DbIterator child) {
    this.child = child;
    this.p = p;
    this.td = child.getTupleDesc();
  }

  public Predicate getPredicate() {
    return p;
  }

  public TupleDesc getTupleDesc() {
    return td;
  }

  public void open() throws DbException, NoSuchElementException,
      TransactionAbortedException {
    child.open();
    super.open();
  }

  public void close() {
    super.close();
    child.close();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    child.rewind();
  }

  /**
   * AbstractDbIterator.readNext implementation. Iterates over tuples from the
   * child operator, applying the predicate to them and returning those that
   * pass the predicate (i.e. for which the Predicate.filter() returns true).
   * 
   * @return The next tuple that passes the filter, or null if there are no
   * more tuples.
   *
   * @see Predicate#filter.
   */
  protected Tuple fetchNext() throws NoSuchElementException,
      TransactionAbortedException, DbException {
    while (child.hasNext()) {
      Tuple t = child.next();

      if (p.filter(t)) {
        return t;
      }
    }
    return null;
  }

  @Override
  public DbIterator[] getChildren() {
    return new DbIterator[] { this.child };
  }

  @Override
  public void setChildren(DbIterator[] children) {
    if (this.child != children[0]) {
      this.child = children[0];
    }
  }

}
