package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor.
 */
public class Insert extends Operator {

  private static final long serialVersionUID = 1L;

  private DbIterator child;

  private final TransactionId tid;
  private final int tableid;
  private final TupleDesc td;

  private int ninserted;
  private boolean alreadyread;

  /**
   * Constructor.
   * 
   * @param tid The transaction running the insert.
   *
   * @param child The child operator from which to read tuples to be inserted.
   *
   * @param tableid The table in which to insert tuples.
   *
   * @throws DbException If TupleDesc of child differs from table into which
   * we are to insert.
   */
  public Insert(TransactionId tid, DbIterator child, int tableid)
      throws DbException {
    this.tid = tid;
    this.child = child;
    this.tableid = tableid;

    this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
    this.ninserted = 0;
    this.alreadyread = false;
  }

  public TupleDesc getTupleDesc() {
    return td;
  }

  public void open() throws DbException, TransactionAbortedException {
    child.open();

    while (child.hasNext()) {
      try {
        Database.getBufferPool().insertTuple(tid, tableid, child.next());
        ninserted += 1;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    super.open();
  }

  public void close() {
    super.close();
    child.close();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    alreadyread = false;
  }

  /**
   * Inserts tuples read from child into the tableid specified by the
   * constructor. It returns a one field tuple containing the number of
   * inserted records. Inserts should be passed through BufferPool. An
   * instances of BufferPool is available via Database.getBufferPool(). Note
   * that insert DOES NOT need check to see if a particular tuple is a
   * duplicate before inserting it.
   * 
   * @return A 1-field tuple containing the number of inserted records, or
   * null if called more than once.
   *
   * @see Database#getBufferPool.
   *
   * @see BufferPool#insertTuple.
   */
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (alreadyread) {
      return null;
    }
    Tuple t = new Tuple(td);
    t.setField(0, new IntField(ninserted));
    alreadyread = true;
    return t;
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
