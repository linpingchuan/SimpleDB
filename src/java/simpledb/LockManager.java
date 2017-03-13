package simpledb;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

class LockManager {

  private enum LockType {
    NO_LOCK,
    SHARED_LOCK,
    EXCLUSIVE_LOCK,
  }

  private class LockState {
    private LockType type;
    private final Condition cond;

    // HashSet of Owners for different locks:
    //  * NO_LOCK, == 0;
    //  * SHARED_LOCK, > 0;
    //  * EXCLUSIVE_LOCK, == 1.
    private final HashSet<TransactionId> owners;
    private final HashSet<TransactionId> waiters;

    private final HashMap<TransactionId, ArrayList<TransactionId>> graph;

    public LockState(Lock m, HashMap<TransactionId, ArrayList<TransactionId>> graph) {
      this.type = LockType.NO_LOCK;
      this.owners = new HashSet<TransactionId>();
      this.waiters = new HashSet<TransactionId>();
      this.cond = m.newCondition();
      this.graph = graph;
    }

    public HashSet<TransactionId> getOwners() {
      return owners;
    }

    public boolean holdsLock(TransactionId tid) {
      return owners.contains(tid);
    }

    public void release(TransactionId tid) {
      if (!owners.contains(tid) || type == LockType.NO_LOCK) {
        return;
      }
      if (type == LockType.EXCLUSIVE_LOCK) {
        assert owners.size() == 1;
        owners.clear();
      } else if (type == LockType.SHARED_LOCK) {
        owners.remove(tid);
      }
      if (owners.isEmpty()) {
        type = LockType.NO_LOCK;
      }
      // Remove me from dependency graph and notify all waiting transactions.
      for (TransactionId waiter : waiters) {
        ArrayList<TransactionId> a = graph.get(waiter);
        assert a != null;
        a.remove(tid);
      }
      cond.signalAll();
    }

    public boolean canGrantShared(TransactionId tid) {
      if (type == LockType.EXCLUSIVE_LOCK) {
        return owners.contains(tid); // Can grant shared lock if owns exclusive.
      }
      return true;
    }

    public boolean canGrantExclusive(TransactionId tid) {
      if (type == LockType.EXCLUSIVE_LOCK) {
        return owners.contains(tid);
      } else if (type == LockType.SHARED_LOCK) {
        assert owners.size() > 0;
        if (owners.size() == 1 && owners.contains(tid)) {
          return true;
        }
        return false;
      }
      return true;
    }

    public boolean acquireShared(TransactionId tid) {
      if (!canGrantShared(tid)) {
        return false;
      }
      type = LockType.SHARED_LOCK;
      if (!owners.contains(tid)) {
        owners.add(tid);
        for (TransactionId waiter : waiters) {
          ArrayList<TransactionId> a = graph.get(waiter);
          if (a == null) {
            a = new ArrayList<TransactionId>();
            graph.put(waiter, a);
          }
          a.add(tid);
        }
      }
      return true;
    }

    public boolean acquireExclusive(TransactionId tid) {
      if (!canGrantExclusive(tid)) {
        return false;
      }
      type = LockType.EXCLUSIVE_LOCK;
      if (!owners.contains(tid)) {
        owners.add(tid);
        for (TransactionId waiter : waiters) {
          ArrayList<TransactionId> a = graph.get(waiter);
          if (a == null) {
            a = new ArrayList<TransactionId>();
            graph.put(waiter, a);
          }
          a.add(tid);
        }
      }
      return true;
    }

    public void wait(boolean shared, TransactionId tid)
        throws TransactionAbortedException {
      if (!waiters.contains(tid)) {
        waiters.add(tid);
        ArrayList<TransactionId> a = graph.get(tid);
        if (a == null) {
          a = new ArrayList<TransactionId>();
          graph.put(tid, a);
        }
        for (TransactionId owner : owners) {
          a.add(owner);
        }
      }
      try {
        if (shared) {
          while (!canGrantShared(tid)) {
            cond.await(1, TimeUnit.SECONDS);
          }
        } else {
          while (!canGrantExclusive(tid)) {
            cond.await(1, TimeUnit.SECONDS);
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new TransactionAbortedException();
      } finally {
        waiters.remove(tid);
        ArrayList<TransactionId> a = graph.get(tid);
        assert a != null;
        for (TransactionId owner : owners) {
          a.remove(owner);
        }
      }
    }
  }

  private final Lock mutex;
  private final HashMap<PageId, LockState> lock;

  /* Lock dependency graph, waiter -> owner. */
  private final HashMap<TransactionId, ArrayList<TransactionId>> graph;

  public LockManager() {
    mutex = new ReentrantLock();
    lock = new HashMap<PageId, LockState>();
    graph = new HashMap<TransactionId, ArrayList<TransactionId>>();
  }

  private boolean isConnected(TransactionId from, TransactionId to) {
    Queue<TransactionId> q = new LinkedList<TransactionId>();
    HashSet<TransactionId> visited = new HashSet<TransactionId>();

    q.add(from);
    visited.add(from);
    while (!q.isEmpty()) {
      TransactionId x = q.poll();
      ArrayList<TransactionId> neighbors = graph.get(x);

      if (neighbors == null) {
        continue;
      }
      for (TransactionId y : neighbors) {
        if (y.equals(to)) {
          return true;
        }
        if (!visited.contains(y)) {
          q.add(y);
          visited.add(y);
        }
      }
    }
    return false;
  }

  private boolean detectDeadLock(LockState lockstate, TransactionId waiter) {
    for (TransactionId owner : lockstate.getOwners()) {
      if (isConnected(owner, waiter)) {
        // There is a path from owner to waiter, if we add an edge from waiter
        // to owner, which will form a cycle in dependency graph, a deadlock in
        // other words.
        return true;
      }
    }
    return false;
  }

  public boolean holdsLock(TransactionId tid, PageId pid) {
    mutex.lock();

    try {
      LockState lockstate = lock.get(pid);

      if (lockstate == null) {
        return false;
      }
      return lockstate.holdsLock(tid);
    } finally {
      mutex.unlock();
    }
  }

  public void release(TransactionId tid, PageId pid) {
    mutex.lock();

    try {
      LockState lockstate = lock.get(pid);

      if (lockstate != null) {
        lockstate.release(tid);
      }
    } finally {
      mutex.unlock();
    }
  }

  // 2-PL requires us to release all locks related to one transaction atomically.
  public void releaseAll(TransactionId tid) {
    mutex.lock();

    try {
      for (LockState lockstate : lock.values()) {
        lockstate.release(tid);
      }
    } finally {
      mutex.unlock();
    }
  }

  public void acquireShared(TransactionId tid, PageId pid)
      throws TransactionAbortedException {
    mutex.lock();

    LockState lockstate = lock.get(pid);

    if (lockstate == null) {
      lockstate = new LockState(mutex, graph);
      lock.put(pid, lockstate);
    }

    while (!lockstate.acquireShared(tid)) {
      if (detectDeadLock(lockstate, tid)) {
        mutex.unlock();
        throw new TransactionAbortedException();
      } else {
        lockstate.wait(true, tid);
      }
    }

    mutex.unlock();
  }

  public void acquireExclusive(TransactionId tid, PageId pid)
      throws TransactionAbortedException {
    mutex.lock();

    LockState lockstate = lock.get(pid);

    if (lockstate == null) {
      lockstate = new LockState(mutex, graph);
      lock.put(pid, lockstate);
    }

    while (!lockstate.acquireExclusive(tid)) {
      if (detectDeadLock(lockstate, tid)) {
        mutex.unlock();
        throw new TransactionAbortedException();
      } else {
        lockstate.wait(false, tid);
      }
    }

    mutex.unlock();
  }
}
