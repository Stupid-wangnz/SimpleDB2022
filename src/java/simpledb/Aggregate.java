package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.afield=afield;
        this.child=child;
        this.gfield=gfield;
        this.aop=aop;

        if(gfield==-1)
            type=null;
        else
            type=child.getTupleDesc().getFieldType(gfield);

        if(child.getTupleDesc().getFieldType(afield)==Type.INT_TYPE)
            aggregator=new IntegerAggregator(gfield,type,afield,aop);
        else
            aggregator=new StringAggregator(gfield,type,afield,aop);


    }
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator iterator;
    private Type type;
    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here

       return gfield;
	//return -1;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if(gfield==-1)
            return null;

        return child.getTupleDesc().getFieldName(gfield);
	//return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
        return afield;

	//return -1;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return child.getTupleDesc().getFieldName(afield);

	//return null;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
        return aop;

	//return null;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here


        super.open();
        child.open();
        while (child.hasNext()){
            Tuple t=child.next();
            //System.out.println(t);
            aggregator.mergeTupleIntoGroup(t);
        }
        iterator=aggregator.iterator();
        iterator.open();
        child.close();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if(!iterator.hasNext())
            return null;
        return iterator.next();
	//return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here

       iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        //组合merge的信息成新的tuple
        if(this.gfield==Aggregator.NO_GROUPING)
            return new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(afield)},new String[]{child.getTupleDesc().getFieldName(afield)});

        Type []types=new Type[2];
        String []strings=new String[2];
        types[0]=child.getTupleDesc().getFieldType(gfield);
        types[1]=child.getTupleDesc().getFieldType(afield);
        strings[0]=child.getTupleDesc().getFieldName(gfield);
        strings[1]=child.getTupleDesc().getFieldName(afield);

        return new TupleDesc(types,strings);
	//return null;
    }

    public void close() {
	// some code goes here
        //child.close();
        super.close();
        iterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
        return new OpIterator[]{child};
	//return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        child=children[0];
    }
    
}
