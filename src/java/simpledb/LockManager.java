package simpledb;

import java.util.*;

class LockManager {

  private enum LockType {
    NO_LOCK,
    SHARED_LOCK,
    EXCLUSIVE_LOCK,
  }

  private class LockState {
    private LockType type;

    // HashSet of Owners for different locks:
    //  * NO_LOCK, == 0;
    //  * SHARED_LOCK, > 0;
    //  * EXCLUSIVE_LOCK, == 1.
    private final HashSet<TransactionId> owners;

    public LockState() {
      this.type = LockType.NO_LOCK;
      this.owners = new HashSet<TransactionId>();
    }

    public boolean holdsLock(TransactionId tid) {
      return owners.contains(tid);
    }

    public void release(TransactionId tid) {
      if (type == LockType.EXCLUSIVE_LOCK) {
        if (owners.contains(tid)) {
          owners.clear();
          type = LockType.NO_LOCK;
        }
      } else if (type == LockType.SHARED_LOCK) {
        owners.remove(tid);
        if (owners.isEmpty()) {
          type = LockType.NO_LOCK;
        }
      }
    }

    public boolean acquireAsShared(TransactionId tid) {
      if (type == LockType.NO_LOCK) {
        type = LockType.SHARED_LOCK;
      } else if (type == LockType.EXCLUSIVE_LOCK) {
        return owners.contains(tid); // Can acquire shared lock if owns exclusive.
      }
      owners.add(tid);
      return true;
    }

    public boolean acquireAsExclusive(TransactionId tid) {
      if (type == LockType.EXCLUSIVE_LOCK) {
        return owners.contains(tid);
      } else if (type == LockType.SHARED_LOCK) {
        if (owners.size() == 1 && owners.contains(tid)) {
          type = LockType.EXCLUSIVE_LOCK; // Upgrade lock.
          return true;
        }
        return false;
      } else {
        type = LockType.EXCLUSIVE_LOCK;
        owners.add(tid);
        return true;
      }
    }
  }

  private final HashMap<PageId, LockState> lock;

  public LockManager() {
    lock = new HashMap<PageId, LockState>();
  }

  public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
    LockState lockstate = lock.get(pid);

    if (lockstate == null) {
      return false;
    }
    return lockstate.holdsLock(tid);
  }

  public synchronized void release(TransactionId tid, PageId pid) {
    LockState lockstate = lock.get(pid);

    if (lockstate != null) {
      lockstate.release(tid);
    }
  }

  // 2-PL requires us to release all locks related to one transaction atomically.
  public synchronized void releaseAll(TransactionId tid) {
    for (LockState lockstate : lock.values()) {
      lockstate.release(tid);
    }
  }

  public synchronized boolean acquireAsShared(TransactionId tid, PageId pid) {
    LockState lockstate = lock.get(pid);

    if (lockstate == null) {
      lockstate = new LockState();
      lock.put(pid, lockstate);
    }
    return lockstate.acquireAsShared(tid);
  }

  public synchronized boolean acquireAsExclusive(TransactionId tid, PageId pid) {
    LockState lockstate = lock.get(pid);

    if (lockstate == null) {
      lockstate = new LockState();
      lock.put(pid, lockstate);
    }
    return lockstate.acquireAsExclusive(tid);
  }
}
