package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that TRANSACTION can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType must be one of LockType.S, LockType.X, and behavior is unspecified
     * if an intent lock is passed in to this method (you can do whatever you want in this case).
     *
     * If TRANSACTION is null, this method should do nothing.
     */



    public static void acquireParent(BaseTransaction transaction, LockType lock, LockContext lockContext) {
        if (lockContext == null) {
            return;
        }
        if (lockContext.getEffectiveLockType(transaction) != LockType.NL) {
            return;
        }
        acquireParent(transaction, lock, lockContext.parentContext());
        lockContext.acquire(transaction, lock);
    }

    public static void promoteParent(BaseTransaction transaction, LockType lock, LockContext lockContext) {
        if (lockContext == null) {
            return;
        } else {
            if (!LockType.substitutable(lock, lockContext.getEffectiveLockType(transaction))) {
                return;
            }
            promoteParent(transaction, lock, lockContext.parentContext());
            lockContext.promote(transaction, lock);
        }
    }

    public static void ensureSufficientLockHeld(BaseTransaction transaction, LockContext lockContext,
                                                LockType lockType) {
        if (transaction == null) {
            return;
        } else if (!(lockType == LockType.S || lockType == LockType.X)) {
            return;
        }
        LockType parentLock = LockType.parentLock(lockType);
        LockType lock = lockContext.getEffectiveLockType(transaction);
        if (lock == lockType) {
            return;
        }
        if (lock == LockType.NL) {
            acquireParent(transaction, parentLock, lockContext.parentContext());
            lockContext.acquire(transaction, lockType);
            return;
        }
        if (LockType.substitutable(lock, lockType)) {
            return;
        }
        if (LockType.substitutable(lockType, lock)) {
            promoteParent(transaction, parentLock, lockContext.parentContext());
            lockContext.promote(transaction, lockType);
            return;
        }
        if (!LockType.substitutable(lockType, lock) && lockContext.saturation(transaction) != 0) {
            if (lockType == LockType.S && lock == LockType.IS) {
                lockContext.escalate(transaction);
                return;
            }
            return;
        }
        if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
            return;
        } else {
            lockContext.escalate(transaction);
            if (lockContext.getEffectiveLockType(transaction) != lockType) {
                promoteParent(transaction, parentLock, lockContext.parentContext());
                lockContext.promote(transaction, lockType);
            }
        }
    }



    // TODO(hw5): add helper methods as you see fit
}
