package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.transactionId=t;
        this.childOpIterator=child;
        this.tableId=tableId;
        called=false;
    }

    TransactionId transactionId;
    OpIterator childOpIterator;
    int tableId;
    boolean called;

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"Inserted Records"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        childOpIterator.open();
    }

    public void close() {
        // some code goes here
        childOpIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOpIterator.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(called)
            return null;
        called=true;

        int count=0;
        while (childOpIterator.hasNext()) {
            Tuple t=childOpIterator.next();
            try {
                Database.getBufferPool().insertTuple(transactionId,tableId,t);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        TupleDesc tupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"Inserted Records"});
        Tuple tuple=new Tuple(tupleDesc);
        tuple.setField(0,new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{childOpIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        childOpIterator=children[0];
    }
}
