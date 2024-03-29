package simpledb;

import java.util.*;

/**
 * BTreeReverseScan is an operator which reads tuples in reverse sorted order
 * according to a predicate
 */
public class BTreeReverseScan implements OpIterator{

    private static final long serialVersionUID = 1L;

    private boolean isOpen = false;
    private TransactionId tid;
    private TupleDesc myTd;
    private IndexPredicate ipred = null;
    private transient DbFileIterator it;
    private String tablename;
    private String alias;
    /**
     * Creates a B+ tree reverse scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     * @param ipred
     * 			  The index predicate to match. If null, the scan will return all tuples
     *            in sorted order
     */
    public BTreeReverseScan(TransactionId tid, int tableid, String tableAlias, IndexPredicate ipred) {
        this.tid = tid;
        this.ipred = ipred;
        reset(tableid, tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return this.tablename;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return this.alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.isOpen=false;
        this.alias = tableAlias;
        this.tablename = Database.getCatalog().getTableName(tableid);
        if(ipred == null) {
            this.it = ((BTreeFile)Database.getCatalog().getDatabaseFile(tableid)).reverseIterator(tid);
        }
        else {
            this.it = ((BTreeFile) Database.getCatalog().getDatabaseFile(tableid)).indexReverseIterator(tid, ipred);
        }
        myTd = Database.getCatalog().getTupleDesc(tableid);
        String[] newNames = new String[myTd.numFields()];
        Type[] newTypes = new Type[myTd.numFields()];
        for (int i = 0; i < myTd.numFields(); i++) {
            String name = myTd.getFieldName(i);
            Type t = myTd.getFieldType(i);

            newNames[i] = tableAlias + "." + name;
            newTypes[i] = t;
        }
        myTd = new TupleDesc(newTypes, newNames);
    }

    public BTreeReverseScan(TransactionId tid, int tableid, IndexPredicate ipred) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid), ipred);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        if (isOpen)
            throw new DbException("double open on one OpIterator.");

        it.open();
        isOpen = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");
        return it.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");

        return it.next();
    }

    @Override
    public void close() {
        it.close();
        isOpen = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return myTd;
    }


}
