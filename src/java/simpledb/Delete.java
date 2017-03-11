package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

  private static final long serialVersionUID = 1L;

  private DbIterator child;

  private final TransactionId tid;
  private final TupleDesc td;

  private int ndeleted;
  private boolean alreadyread;

  /**
   * Constructor specifying the transaction that this delete belongs to as
   * well as the child to read from.
   * 
   * @param tid The transaction this delete runs in.
   *
   * @param child The child operator from which to read tuples for deletion.
   */
  public Delete(TransactionId tid, DbIterator child) {
    this.tid = tid;
    this.child = child;

    this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
    this.ndeleted = 0;
    this.alreadyread = false;
  }

  public TupleDesc getTupleDesc() {
    return td;
  }

  public void open() throws DbException, TransactionAbortedException {
    child.open();

    while (child.hasNext()) {
      try {
        Database.getBufferPool().deleteTuple(tid, child.next());
        ndeleted += 1;
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
   * Deletes tuples as they are read from the child operator. Deletes are
   * processed via the buffer pool (which can be accessed via the
   * Database.getBufferPool() method.
   * 
   * @return A 1-field tuple containing the number of deleted records.
   *
   * @see Database#getBufferPool.
   *
   * @see BufferPool#deleteTuple.
   */
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (alreadyread) {
      return null;
    }
    Tuple t = new Tuple(td);
    t.setField(0, new IntField(ndeleted));
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
