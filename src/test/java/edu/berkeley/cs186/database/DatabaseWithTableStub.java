package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockManager;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.TableStub;

public class DatabaseWithTableStub extends Database {
    public DatabaseWithTableStub(String fileDir) throws DatabaseException {
        super(fileDir);
    }

    public DatabaseWithTableStub(String fileDir, int numMemoryPages) throws DatabaseException {
        super(fileDir, numMemoryPages);
    }

    public DatabaseWithTableStub(String fileDir, int numMemoryPages,
                                 LockManager lockManager) throws DatabaseException {
        super(fileDir, numMemoryPages, lockManager);
    }

    @Override
    protected Table newTable(String tableName, Schema schema, String fileName, LockContext lockContext,
                             BaseTransaction transaction) {
        return new TableStub(tableName, schema, fileName, lockContext, transaction);
    }

    @Override
    protected Table newTable(String tableName, String fileName, LockContext lockContext,
                             BaseTransaction transaction)
    throws DatabaseException  {
        return new TableStub(tableName, fileName, lockContext, transaction);
    }
}
