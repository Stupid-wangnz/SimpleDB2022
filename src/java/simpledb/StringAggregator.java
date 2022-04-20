package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        hashMap= new HashMap<Field,Integer>();
        fieldNames=new String[2];

        if (what != Op.COUNT)
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
    }
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field,Integer>hashMap;
    private String [] fieldNames;

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield!=NO_GROUPING)
            fieldNames[0]=tup.getTupleDesc().getFieldName(gbfield);
        fieldNames[1]=tup.getTupleDesc().getFieldName(afield);

        Field aField=tup.getField(this.afield);
        Field gbField=gbfield==NO_GROUPING?null:tup.getField(this.gbfield);

        if(!hashMap.containsKey(gbField)){
            hashMap.put(gbField,1);
        }
        else
            hashMap.put(gbField,hashMap.get(gbField)+1);

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator();
    }
    private class StringAggregatorIterator implements OpIterator{

        private HashMap<Field,Integer> hashMap;
        private Iterator<Tuple> iterator;
        private TupleDesc tupleDesc;
        private ArrayList<Tuple> TupleList;

        StringAggregatorIterator(){
            hashMap=StringAggregator.this.hashMap;
            Type []type=new Type[2];
            String [] fieldName=new String[2];
            type[0]=gbfieldtype;
            type[1]=Type.INT_TYPE;
            fieldName[0]=fieldNames[0];
            fieldName[1]=fieldNames[1];
            if(gbfield!=NO_GROUPING)
                tupleDesc=new TupleDesc(type,fieldName);
            else
                tupleDesc=new TupleDesc(new Type[]{type[1]},new String[]{fieldNames[1]});
            TupleList=new ArrayList<>();
            for(Field field:hashMap.keySet()){
                Tuple tuple=new Tuple(tupleDesc);
                if(gbfield!=NO_GROUPING) {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(hashMap.get(field)));
                }
                else {
                    tuple.setField(0,new IntField(hashMap.get(field)));
                }
                TupleList.add(tuple);
            }
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterator=TupleList.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            iterator=null;
        }
    }
}
