package edu.berkeley.cs186.database.concurrency;


import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.Database;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via BaseTransaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // You may add helper methods here if you wish
    }

    // You should not modify or use this directly.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // You may add helper methods here if you wish

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {
            acquire(transaction, name, lockType);
            if (transaction.getBlocked()) {
                try {
                    promote(transaction, name, lockType);
                }
                catch (DuplicateLockRequestException | NoLockHeldException | InvalidLockException i) {
                    return;
                }
                transaction.unblock();
                return;
            }
            for (ResourceName n : releaseLocks) {
                release(transaction, n);
            }
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean blocked = false;
        synchronized (this) {
            ResourceEntry resEnt = getResourceEntry(name);
            Long transNum = transaction.getTransNum();
            Lock lock = new Lock(name,lockType, transNum);
            // if there are no locks add the lock
            if (checkConflicts(lock) == null  && resEnt.waitingQueue.isEmpty()) {
                resEnt.locks.add(lock);
                addTransactionLocks(lock);
            }
            // if there are locks check to see if its compatible
            // if not compatible add to que and block transaction, else add to locks
            else {
                Lock oldLock = checkConflicts(lock);
                if (oldLock != null) {
                    if (oldLock.lockType == lockType && oldLock.transactionNum == transNum) {
                        throw new DuplicateLockRequestException("No duplicate locks");
                    }
                }
                resEnt.waitingQueue.add(new LockRequest(transaction, lock));
                blocked = true;
            }
        }
        // block transaction if added to the que.
        if (blocked) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            //if no lock held, throw an exception
            if (getLockType(transaction, name) == LockType.NL){
                throw new NoLockHeldException("No Lock held");
            }
            // else remove the lock from the list of the locks, add the que lock, remove the que lock from the que lock list.
            ResourceEntry resEnt = getResourceEntry(name);
            List<Lock> resLocks = resEnt.locks;
            long transNum = transaction.getTransNum();
            Lock lock = getLock(transNum, resLocks);

            if (lock != null) {
                resLocks.remove(lock);
                removeTransactionLocks(lock);
                if (resEnt.waitingQueue.isEmpty()) {
                    return;
                }
                LockRequest queLockRequest = resEnt.waitingQueue.getFirst();
                Lock queLock = queLockRequest.lock;
                Lock queConflictLock = checkConflicts(queLock);
                if (promote && queConflictLock != null) {
                    promote(queLockRequest.transaction, queLockRequest.lock.name, queLock.lockType);
                    resEnt.waitingQueue.removeFirst();
                    promote = false;
                }
                //if no conflict add the que lock
                if (queConflictLock == null) {
                    getResourceEntry(queLock.name).locks.add(queLock);
                    resEnt.waitingQueue.removeFirst();
                    addTransactionLocks(queLock);
                    releaseLockRequest(queLockRequest);
                    queLockRequest.transaction.unblock();
                }
            }
        }
        transaction.unblock();
    }

    public Lock checkConflicts(Lock lock) {
        ResourceName name = lock.name;
        long transNum = lock.transactionNum;
        ResourceEntry resEnt = getResourceEntry(name);
        List<Lock> resLocks = getLocks(name);
        if (resLocks.isEmpty()) {
            return null;
        }
        for (Lock oldLock : resLocks) {
            if (!LockType.compatible(oldLock.lockType, lock.lockType)) {
                return oldLock;
            }
        }
        if (!resEnt.waitingQueue.isEmpty()) {
            return null;
        }

        return null;

    }


    public Lock getLock(long transactionNum, List<Lock> resourceLocks) {
        for (Lock lock : resourceLocks) {
            if (lock.transactionNum == transactionNum) {
                return lock;
            }
        }
        return null;
    }


    public void removeTransactionLocks(Lock lock) {
        List<Lock> transLocks = transactionLocks.getOrDefault(lock.transactionNum, new ArrayList<Lock>());
        transLocks.remove(lock);
        if (transLocks.isEmpty()) {
            transactionLocks.remove(lock.transactionNum);
        } else {
            transactionLocks.put(lock.transactionNum, transLocks);
        }
    }

    public void addTransactionLocks(Lock lock) {
        List<Lock> transLocks = transactionLocks.getOrDefault(lock.transactionNum, new ArrayList<Lock>());
        transLocks.add(lock);
        transactionLocks.put(lock.transactionNum, transLocks);
    }


    public void releaseLockRequest(LockRequest lockRequest) {
        if (lockRequest.releasedLocks.isEmpty()) {
            return;
        }
        for (Lock lock : lockRequest.releasedLocks) {
            ResourceEntry resEnt = getResourceEntry(lock.name);
            LockRequest queLockRequest = resEnt.waitingQueue.getFirst();
            Lock queLock = queLockRequest.lock;

            getResourceEntry(lock.name).locks.remove(lock);
            removeTransactionLocks(lock);

            if (checkConflicts(queLock) == null) {
                getResourceEntry(queLock.name).locks.add(queLock);
                addTransactionLocks(queLock);
                resEnt.waitingQueue.removeFirst();
                releaseLockRequest(queLockRequest);
            }
        }

    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */



    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        synchronized (this) {

            ResourceEntry resEnt = getResourceEntry(name);
            List<Lock> resLocks = resEnt.locks;
            Lock lock = getLock(transaction.getTransNum(), resLocks);
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());

            if (lock == null) {
                throw new NoLockHeldException("No lock held");
            }

            if (lock.lockType == newLockType) {
                throw new DuplicateLockRequestException("No duplicate lockType requests");
            }

            if (!LockType.substitutable(newLockType, lock.lockType)) {
                throw new InvalidLockException("This lock is not substitutable");
            }
            resLocks.remove(lock);
            if (checkConflicts(newLock) == null) {
                lock.lockType = newLockType;
                resLocks.add(lock);
                return;
            }
            resLocks.add(lock);
            List<Lock> locks = new ArrayList<Lock>();
            locks.add(lock);
            LockRequest request = new LockRequest(transaction, newLock, locks);
            promote = true;
            resEnt.waitingQueue.addFirst(request);
        }
    }
    public boolean promote = false;
    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(BaseTransaction transaction, ResourceName name) {
        List<Lock> lockList = getLocks(name);
        if (lockList.isEmpty()) {
            return LockType.NL;
        }
        for (Lock lock : lockList) {
            if (lock.transactionNum == transaction.getTransNum()) {
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(BaseTransaction transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        contexts.putIfAbsent("database", new LockContext(this, null, "database"));
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public synchronized LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        contexts.putIfAbsent(name, new LockContext(this, null, name));
        return contexts.get(name);
    }
}
