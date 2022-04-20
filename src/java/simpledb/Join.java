package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        joinPredicate=p;
        opIterator1=child1;
        opIterator2=child2;
        t1=null;
    }
    private JoinPredicate joinPredicate;
    private OpIterator opIterator1;
    private OpIterator opIterator2;
    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return opIterator1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return opIterator2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        return TupleDesc.merge(opIterator1.getTupleDesc(),opIterator2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here

        opIterator2.open();
        opIterator1.open();
        super.open();

    }

    public void close() {
        // some code goes here

        opIterator1.close();
        opIterator2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        //super.rewind();
        opIterator2.rewind();
        opIterator1.rewind();
        t1=null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    Tuple t1;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        while (opIterator1.hasNext()||t1!=null){
            if (t1==null)
                t1=opIterator1.next();

            //Tuple t1=opIterator1.next();
            while (opIterator2.hasNext()) {
                Tuple t2 = opIterator2.next();
                if (joinPredicate.filter(t1, t2)) {
                    Tuple res_t = new Tuple(this.getTupleDesc());
                    for (int i = 0; i < t1.getTupleDesc().numFields(); i++)
                        res_t.setField(i, t1.getField(i));
                    for (int i = t1.t1.getTupleDesc().numFields(); i < t1.t1.getTupleDesc().numFields() + t2.t1.getTupleDesc().numFields(); i++)
                        res_t.setField(i, t2.getField(i - t1.t1.getTupleDesc().numFields()));

                    return res_t;
                }
            }
            //不知道为什么这种格式不行，逻辑上一个意思
            /*if(opIterator1.hasNext())
                t1=opIterator1.next();*/
            t1=null;
            opIterator2.rewind();

        }


        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{opIterator1,opIterator2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        opIterator1=children[0];
        opIterator2=children[1];

    }

}
