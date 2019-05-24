package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if (a == NL || b == NL) {
            return true;
        }
        if (a == IS && (b == IS || b == IX || b == S || b == SIX)) {
            return true;
        }

        if (a == IX && (b == IS || b == IX )) {
            return true;
        }
        if (a == S && (b == IS || b == S)) {
            return true;
        }
        if (a == SIX && b == IS) {
            return true;
        }
        return false;

    }



    /**
     * This method returns the least permissive lock on the parent resource
     * that must be held for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        if (a == X) {
            return IX;
        }
        if (a == NL) {
            return NL;
        }
        if (a == IS) {
            return IS;
        }

        if (a == IX) {
            return IX;
        }
        if (a == S) {
            return IS;
        }
        if (a == SIX) {
            return IX;
        }
        throw new UnsupportedOperationException("bad lock type");
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        if (required == substitute) {
            return true;
        }
        if (required == NL) {
            return true;
        }
        if (required == S && (substitute == X || substitute == SIX)) {
            return true;
        }
        if (required == IS && (substitute == IX || substitute == SIX)) {
            return true;
        }
        if (required == IX && (substitute == SIX || substitute == X)) {
            return true;
        }
        return false;

    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

