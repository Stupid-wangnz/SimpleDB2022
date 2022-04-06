package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        transactionId=t;
        childOpIterator=child;
        tupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"Deleted Records"});
        called=false;
    }
    TransactionId transactionId;
    OpIterator childOpIterator;
    TupleDesc tupleDesc;
    boolean called;

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        childOpIterator.open();
        called=false;
    }

    public void close() {
        // some code goes here
        super.close();
        childOpIterator.close();
        called=true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOpIterator.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        if(called)
            return null;

        called=true;
        int count=0;
        while(childOpIterator.hasNext()){
            Tuple tuple=childOpIterator.next();
            count++;
            try {
                Database.getBufferPool().deleteTuple(transactionId,tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
