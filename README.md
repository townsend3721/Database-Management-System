# Homework 5: Locking

## Logistics

This homework is divided into two parts.

Part 1 is due: **Wednesday 4/24/2019, 11:59:59 PM**

Part 2 is due: **Wednesday 5/1/2019, 11:59:59 PM**

Part 2 requires Part 1 to be completed first. See the Grading section at the 
bottom of this document for notes on how your score will be computed.

**You will not be able to use slip minutes for the Part 1 deadline.** The standard late penalty
(33%/day, counted in days not minutes) will apply after the Part 1 deadline. Slip minutes
may be used for the Part 2 deadline, and the late penalty for the two parts are independent.

For example, if you submit Part 1 on 4/26/2019, 5:30 AM and Part 2 on 5/2/2019 6:20 PM, and have
24 hours of slip minutes, you will receive:
- 66% penalty on your Part 1 submission
- No penalty on your Part 2 submission

## Overview

In this assignment, you will implement multigranularity locking and integrate
it into the codebase.

## Fetching the Assignment

Similar to previous homeworks, to avoid redownloading our maven dependencies every time we start our docker container, we will start our docker container. The first time you work on this homework run the following command:
```
docker run --name cs186hw5 -v <pathname-to-directory-on-your-machine>:/cs186 -it cs186/environment bash
```

Now startup your container again with the following command:
```
docker start cs186hw5
```

The only thing that should happen is the terminal should print cs186hw5. After running those 2 commands once, to open your container in the future the only command you need to run is:
```
docker exec -it cs186hw5 bash
```

While inside your container, navigate to the shared directory:
```
cd cs186
```

Clone the HW5 repo:
```
git clone https://github.com/berkeley-cs186/Sp19HW5.git
```
If you get an error like: `Could not resolve host: github.com`, try restarting your docker machine (run `docker-machine restart` after exiting the docker container), and if that doesn't work restart your entire computer.

If you get an error like fatal: could not create work tree dir: Permission denied, run
```
sudo git clone https://github.com/berkeley-cs186/Sp19HW5.git
```

Before submitting your assignment you must run `mvn clean test` and ensure it works in the docker container.
**We will not accept "the test ran in my IDE" as an excuse.** You should be running the maven tests periodically as you work through the project.

To compile and test your code, run:
```bash
mvn clean compile
mvn clean test -D HW=5
```

If you see this error:
```
Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.2:compile (default-compile) on project database: Fatal error compiling: directory not found: /cs186/Sp19HW5/target/classes
```
run
```bash
sudo mvn clean compile
sudo mvn clean test -D HW=5
```

You should see 67 tests run, 67 errors, and 0 skipped tests.

To run just Part 1 tests, run:
```bash
mvn clean test -D HW=5Part1
```

To run just Part 2 tests, run:
```bash
mvn clean test -D HW=5Part2
```

## Part 0: Understand the Skeleton Code

Read through all of the code in the `concurrency` directory. Many comments contain
critical information on how you must implement certain functions. If you do not obey
the comments, you will lose points.

Try to understand how each class fits in: what is each class responsible for, what are
all the methods you have to implement, and how does each one manipulate the internal state.
Trying to code one method at a time without understanding how all the parts of the lock
manager work often results in having to rewrite significant amounts of code.

The skeleton code divides multigranularity locking into three layers.
- The `LockManager` object manages all the locks, treating each resource as independent
  (it doesn't consider the relationship between two resources: it does not understand that
  a table is at a lower level in the hierarchy than the database, and just treats locks on
  both in exactly the same way). This level is responsible for blocking/unblocking transactions
  as necessary, and is the single source of authority on whether a transaction has a certain lock. If
  the `LockManager` says T1 has X(database), then T1 has X(database).
- A collection of `LockContext` objects, which each represent a single lockable object
  (e.g. a page or a table) lies on top of the `LockManager`. The `LockContext` objects
  are connected according to the hierarchy (e.g. a `LockContext` for a table has the database
  context as its parent, and its pages' contexts as children). The `LockContext` objects
  all share a single `LockManager`, and enforces multigranularity constraints on all
  lock requests (e.g. an exception should be thrown if a transaction attempts to request
  X(table) without IX(database)).
- A declarative layer lies on top of the collection of `LockContext` objects, and is responsible
  for acquiring all the intent locks needed for each S or X request that the database uses
  (e.g. if S(page) is requested, this layer would be responsible for requesting IS(database), IS(table)
  if necessary).

![Layers of locking in HW5](hw5-layers.png?raw=true "Layers")

In Part 1, you will be implementing the bottom two layers (`LockManager` and `LockContext`).

In Part 2, you will be implementing the top layer (`LockUtil`) and integrating everything into the codebase.

## Part 1: LockManager and LockContext

[Part 1 README](hw5-part1-README.md)

## Part 2: LockUtil and integration

[Part 2 README](hw5-part2-README.md)

## Submitting the Assignment

See Piazza for submission instructions.

You may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods.

You should make sure that all code you modify belongs to files with HW5 todo comments in them
(e.g. don't add helper methods to PNLJOperator). A full list of files that you may modify follows:

- concurrency/LockType.java
- concurrency/LockManager.java
- concurrency/LockContext.java
- concurrency/LockUtil.java (Part 2 only)
- index/BPlusTree.java (Part 2 only)
- io/Page.java (Part 2 only)
- io/PageAllocator.java (Part 2 only)
- table/Table.java (Part 2 only)
- Database.java (Part 2 only)

Make sure that your code does *not* use any static variables - this may cause odd behavior when
running with maven vs. in your IDE (tests run through the IDE often run with a new instance
of Java for each test, so the static variables get reset, but multiple tests per Java instance
may be run when using maven, where static variables *do not* get reset).

## Testing

We strongly encourage testing your code yourself, especially after each part (rather than all at the end). The given
tests for this project (even more so than previous projects) are **not** comprehensive tests: it **is** possible to write
incorrect code that passes them all.

Things that you might consider testing for include: anything that we specify in the comments or in this document that a method should do
that you don't see a test already testing for, and any edge cases that you can think of. Think of what valid inputs
might break your code and cause it not to perform as intended, and add a test to make sure things are working.

To help you get started, here is one case that is **not** in the given tests (and will be included in the hidden tests): deleting a table that does not exist should still result in a lock being obtained.

To add a unit test, open up the appropriate test file (all test files are located in `src/test/java/edu/berkeley/cs186/database`
or subdirectories of it), and simply add a new method to the test class, for example:

```
@Test
public void testDatabaseSLock() {
    // your test code here
}
```

Many test classes have some setup code done for you already: take a look at other tests in the file for an idea of how to write the test code.

## Grading
- **20% of your overall score will come from your submission for Part 1**. We will only be running released Part 1 tests (`database.concurrency.TestLockType`, `database.concurrency.TestLockManager`, `database.concurrency.TestLockContext`) on your Part 1 submission.
- The rest of your score will come from your submission for Part 2 (testing both Part 1 and Part 2 code).
- 50% of your overall score will be made up of the tests released in this homework (the tests that we provided
  in `database.concurrency.*` and `database.TestDatabaseLocking`).
- 50% of your overall score will be made up of hidden, unreleased tests that we will run on your submission after the deadline.
- Part 1 is worth 60% of your HW5 grade and Part 2 is worth 40% of your HW5 grade.
