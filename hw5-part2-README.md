# Homework 5: Locking (Part 2)

**A working implementation of Part 1 is required for Part 2. If you have not yet finished
[Part 1](hw5-part1-README.md), you should do so before continuing.**

## Overview

In this part, you will implement the declarative layer (in LockUtil) and integrate
locking into the rest of the codebase.

## LockUtil (declarative constructs)

The LockContext class enforces multigranularity constraints for us, but it's a bit
cumbersome to use in our database: wherever we want to request some locks, we have to
handle requesting the appropriate intent locks, etc.

To simplify integrating locking into our codebase (the second half of this part), we define the
`ensureSufficientLockHeld` method. This method is used like a declarative statement. For example,
let's say we have some code that reads an entire table. To add locking, we can do:

```java
LockUtil.ensureSufficientLockHeld(transaction, tableContext, LockType.S);

// any code that reads the table here
```

After the `ensureSufficientLockHeld` line, we can assume that `transaction` has permission
to read the resource represented by `tableContext`, as well as any children (all the pages).

We can call it several times in a row:

```java
LockUtil.ensureSufficientLockHeld(transaction, tableContext, LockType.S);
LockUtil.ensureSufficientLockHeld(transaction, tableContext, LockType.S);

// any code that reads the table here
```

or write several statements in any order:


```java
LockUtil.ensureSufficientLockHeld(transaction, pageContext, LockType.S);
LockUtil.ensureSufficientLockHeld(transaction, tableContext, LockType.S);
LockUtil.ensureSufficientLockHeld(transaction, pageContext, LockType.S);

// any code that reads the table here
```

and no errors should be thrown, and at the end of the calls, we should be able to read all of
the table.

Note that the caller does not care exactly which locks the transaction actually has: if we
gave the transaction an X lock on the database, the transaction would indeed have permission to
read all of the table. But this doesn't allow for much concurrency (and actually enforces
a serial schedule if used with 2PL), so we additionally stipulate that `ensureSufficientLockHeld`
should grant as little additional permission as possible: if an S lock suffices, we should
have the transaction acquire an S lock, not an X lock, but if the transaction already has an X lock,
we should leave it alone (`ensureSufficientLockHeld` should never reduce the permissions
a transaction has; it should always let the transaction do at least as much as it used to, before
the call).

We suggest breaking up the logic of this method into two phases: ensuring that we have the appropriate
locks on ancestors, and acquiring the lock on the resource. You will need to promote in some cases,
and escalate in some cases (these cases are not mutually exclusive).

Note on redundant locks: if there are redundant locks left (for example, S locks on children
after promoting IX -> SIX), do not release the redundant locks (just leave them be).

#### Additional Notes

After this, you should pass all the tests we have provided to you under `database.concurrency.*`.

You are strongly encouraged to test your code further with your own tests before continuing to the
next part (see the Testing section for instructions on how). Although you will not be graded
on testing, the provided tests are in no sense comprehensive, and bugs in this section **will**
cause additional difficulties in implementing and testing the next part of the homework (this
was the case for very many students in past semesters).

Note that you may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods. Also, you should only modify code in
the `concurrency` directory for this section.

## Integrating the Lock Manager and Lock Context

At this point, you should have a working, standalone lock manager and context. In
this section, you will modify code throughout the codebase to use locking.

This section will be mostly concerned with code in `database.io.*`, `database.table.Table`, and with the Database
and Transaction classes. We strongly recommend understanding how the codebase
is structured (described below) before starting, and maybe drawing out a diagram
of the relevant classes - it will save you a lot of time understanding the tasks.

The `database.io` package contains the code that actually interfaces with the disk.
- The `Page` class represents a single page of data on disk and loaded into memory. The
  `PageBuffer` inner class implements the `Buffer` interface, and is how someone with a `Page` object
  is able to read and write to it. All reads are funnelled through the `Page.PageBuffer#get` method, and
  all writes are funnelled through the `Page.PageBuffer#put` method.
- The `PageAllocator` class is essentially the buffer manager, except specific to only a single table
  or index's pages (there is exactly one `PageAllocator` for each table and for each index). The `PageAllocator`
  manages the pages a table or index has, and loads them into/out of memory as needed. It passes
  the appropriate page-level lock context to any `Page` object it constructs (these are just
  `tableContext.childContext(pageNum)` calls).

  The `PageAllocator` allocates a number of header pages as needed (the total number of which is stored
  in `numUsedHeaderPages`) to manage the data pages (counted in `numPages`). You may see throughout this
  part of the homework that more than `n` pages are locked when `n` is the number of data pages - this is
  due to the header pages being locked.

The `database.table` package contains table-related code.
- The `Table` class represents a table. It creates a `PageAllocator` for its pages in its constructors,
  and passes its own lock context to the `PageAllocator` (i.e. the `PageAllocator`'s lock context is
  the same as the table's).

The `database.index` package contains index-related code.
- The `BPlusTree` class represents a B+ tree index. It creates a `PageAllocator` for its pages in its constructors,
  and passes its own lock context to the `PageAllocator` (i.e. the `PageAllocator`'s lock context is
  the same as the index's).

In the `database` package we have the `Database` class.
- The `Database` class is the object that users of our database library interact with. The `Database` class
  represents the entire database, and manages the tables and indices of the database (creating Table/BPlusTree
  objects as needed and passing the appropriate table/index-level lock context to them). It contains a `Transaction`
  inner class which represents a single transaction. All queries and changes in our database are done
  through transactions (and this is where much of the integration work will take place).

  In the constructor of `Database`, a transaction with transaction number -1 is created and given an X(database) lock
  before the tables and indices are loaded. This transaction ends at the end of the constructor, and therefore
  starts/finishes at the start of _every_ test in this section. Likewise, in the `close()` method of `Database`, a
  transaction with transaction number -2 is created and given an X(database) lock  to clean everything up, and this
  is called at least once per test. If you see conflicts in this section with an X(database)
  lock, you are likely not properly releasing locks at the end of a transaction.

Note: most of these tasks only require writing a few lines of code. Errors and incorrect output
are very frequently the result of uncaught bugs in `LockUtil`.

#### Task 1: Page-level locking

The simplest scheme for locking is to simply lock pages as we need them. As all reads and
writes to pages are performed via the `Page.PageBuffer` class, it suffices to
change only that.

Modify the `get` and `put` methods of `Page.PageBuffer` to lock the page (and
acquire locks up the hierarchy as needed) with the least permissive lock
types possible (Hint: the declarative layer may come in handy).

Note: no more tests will pass after doing this, see the next task for why.

You should modify `database/io/Page.java` for this task (PageBuffer is an
inner class of Page).

#### Task 2: Releasing Locks

At this point, transactions should be acquiring lots of locks needed to do their
queries, but no locks are ever released, which means that
our database won't be able to do much after someone requests an X lock...and we do
just that when initializing the database!

We will be using Strict Two-Phase Locking in our database, which means that lock
releases only happen when the transaction finishes, in the `end` method.

Modify the `end` method of `Database.Transaction` to release all locks the
transaction acquired.

You should only use `LockContext#release` and not `LockManager#release` - `LockManager`
will not verify multigranularity constraints, but other transactions at the same time
assume that these constraints are met, so you do want these constraints to be maintained.

Hint: you can't just release the locks in any order! Think about in what order you are allowed to release
the locks.

You should modify `database/Database.java` for this task (Transaction is an inner class
of Database), and should pass `testRecordRead` and `testSimpleTransactionCleanup` after
implementing this task and task 1.

#### Task 3: Writes

At this point, you have successfully added locking to the database! This task,
and the remaining integration tasks, are focused on locking in a more efficient manner.

The first operation that we perform in an insert/update/delete of
a record is reading a bitmap. Following our logic of reactively acquiring locks
only as needed, we would request an S lock on the page when reading the bitmap,
then request a promotion to an X lock when attempting to write to the page.
Given that we know we will eventually need an X lock,
we should request it to begin with.

Modify `addRecord`, `updateRecord`, `deleteRecord`, and `cleanup` in `Table` to
request the appropriate X lock upfront.

Note: the `cleanup` method is a table-level operation that releases empty pages (resulting
from deleting lots of records). What lock is appropriate for this operation?

You should modify `database/table/Table.java` for this task, and should
pass `testRecordWrite`, `testRecordUpdate`, `testRecordDelete`, and `testTableCleanup` after
implementing this task.

#### Task 4: Scans

Our approach of locking every page can be inefficient:
a full table scan over a table of 3000 pages will require acquiring over 3000 locks.

Reduce the locking overhead when performing table scans, by locking the
entire table when any of `Table#ridIterator`, `Database.Transaction#sortedScan`, and
`Database.Transaction#sortedScanFrom` is called.

A useful methods for this task is: `Database#getTableContext`.

You should modify `database/table/Table.java` and `database/Database.java` for this task,
and should pass `testTableScan` and `testSortedScanNoIndexLocking` after implementing this task.

#### Task 5: Indices

Locking pages as we read or write from them works for simple queries. But this is rather
inefficient for indices: if you use an index in any way, locking page by page, you
acquire around `h` locks (where `h` is the height of the index), but effectively have locked
the entire B+ tree (think about why this is the case!), which is bad for concurrency.

There are many ways to lock B+ trees efficiently (one simple method is known as "crabbing",
see the textbook for details and other approaches), but for
simplicity, we are just going to lock the entire tree for both reads and writes (locking page
by page with strict 2PL already acts as a lock on the entire tree, so we may as well just
get a lock on the whole tree to begin with). This is no better for concurrency, but at least reduces the locking overhead.

First, modify BPlusTree constructors to disable locking at lower granularities
(take a look at the methods in `LockContext` for this).

Then, preemptively acquire locks in the following
methods in `BPlusTree`: `scanEqual`, `scanAll`, `scanGreaterEqual`, `put`, `bulkLoad`, `remove`,
`toSexp`, `toDot`; and the constructors of `BPlusTree`.

You should modify `database/index/BPlusTree.java` for this task, and
should pass `testBPlusTreeRestrict`, `testSortedScanLocking`, `testSearchOperationLocking`, and `testQueryWithIndex`
after implementing this task.

#### Task 6: Temporary Tables

As you may recall from HW3 and HW4, there are various places in more complex queries where
we write intermediate relations into a temporary table. At the moment, our database locks
on every single page we touch in the table, even though we know that only one transaction
even has access to the temporary table.

Modify `createTempTable` in `Database.Transaction` to disable locking at lower granularities,
and then acquire an appropriate lock on the temporary table. (What lock is appropriate here?)

You should modify `database/Database.java` for this task, and should pass `testTempTable` after
implementing this task.

#### Task 7: Auto-escalation

Although we already optimized table scans to lock the entire table, we may still run into cases
where a single transaction holds locks on a significant portion of a table's pages.

To do anything about this problem, we must first have some idea of how many pages a table uses:
if a table had 1000 pages, and a transaction holds locks on 900 pages, it's a problem, but if a table
had 100000 pages, then a transaction holding 900 page locks is something we can't really optimize.
Begin by modifying methods `PageAllocator` (in `database/io/PageAllocator.java`) to set the capacity of
the table-level lock contexts appropriately (to the number of children - data and header pages - that the table has).

Once we have a measure of how many pages a table has, and how many pages of it a transaction holds locks on,
we can implement a simple policy to automatically escalate page-level locks into a table-level lock. You
should modify the codebase to escalate locks from page-level to table-level when both of two conditions are met:

1. The transaction holds at least 20% of the table's pages.
2. The table has at least 10 pages (to avoid locking the entire table unnecessarily when the table is very small).

Auto-escalation should only happen when a transaction requests a new lock on the table's pages, where both of the
above conditions are met *before* the new request is considered. You should escalate the lock first, then request the
new lock only if the escalated lock is insufficient.

This task is more open-ended than the other tasks. You may modify any class with a HW5 todo in it (including
classes from Part 1) by either modifying existing methods or adding new public or private methods. Your code must still pass Part 1 tests,
so it is recommended that you backup your implementations of Part 1 if you opt to modify those classes.

Hint: when are we automatically escalating locks? Think about where you can place the auto-escalation code to ensure
that we always escalate when we need to.

A full list of files you can modify: `database/concurrency/LockType.java`, `database/concurrency/LockManager.java`,
`database/concurrency/LockContext.java`, `database/concurrency/LockUtil.java`, `database/index/BPlusTree.java`, `database/io/Page.java`,
`database/io/PageAllocator.java`, `database/table/Table.java`, `database/Database.java`.

You should pass `testPageAllocatorInitCapacity`, `testAutoEscalateS`, and `testAutoEscalateX` after
implementing this task.

#### Task 8: Transactional DDL

So far (aside from locking temporary table creation), we have been focusing on locking DML\* operations.
We'll now focus on locking DDL\* operations. Our database supports transactional DDL, which is to say,
we *allow* operations like table creation to be done during a transaction.

There are four operations that our database supports that fall under DDL: `createTable`, `createTableWithIndices`,
`deleteTable`, and `deleteAllTables` (all methods of `Database.Transaction`). Carefully consider when in each method
you should acquire locks, and modify them to request the appropriate locks.

You should modify `database/Database.java` for this task, and should pass `testCreateTableSimple`, `testDeleteTableSimple`,
and `testDeleteAllTables` after implementing this task.

\*DML and DDL are terms you might remember from the first few lectures. DML is the Data Manipulation Language, which
encompasses operations that play with the data itself (e.g. inserts, queries), whereas DDL is the Data Definition
Language, which encompasses operations that touch the *structure* of the data (e.g. table creation, changes to the
schema).

#### Additional Notes

At this point, you should pass all the test cases in `database.TestDatabaseLocking`.

Note that you may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods.

You should make sure that all code you modify belongs to files with HW5 todo comments in them
(e.g. don't add helper methods to PNLJOperator). A full list of files that you may modify follows:

- concurrency/LockType.java
- concurrency/LockManager.java
- concurrency/LockContext.java
- concurrency/LockUtil.java
- index/BPlusTree.java
- io/Page.java
- io/PageAllocator.java
- table/Table.java
- Database.java
