package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    private ArrayList<TDItem> tdItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tdItems=new ArrayList<>();

        for(int i=0;i<typeAr.length;i++)
        {
            TDItem temItem;
            if(i< fieldAr.length)
                temItem=new TDItem(typeAr[i],fieldAr[i]);
            else
                temItem=new TDItem(typeAr[i],null);
            tdItems.add(temItem);
        }
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdItems=new ArrayList<>();

        for(int i=0;i<typeAr.length;i++)
        {
            TDItem temItem;
            temItem=new TDItem(typeAr[i],null);
            tdItems.add(temItem);
        }

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i>=tdItems.size()||i<0)
            throw new NoSuchElementException();

        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i>=tdItems.size()||i<0)
            throw new NoSuchElementException();
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        for(int i=0;i<tdItems.size();i++) {
            if(tdItems.get(i).fieldName==null)
                if(name==null)
                    return i;
                else
                    continue;
            if (tdItems.get(i).fieldName.equals(name)) {
                return i;
            }
        }

        throw new NoSuchElementException();
        //return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sum=0;
        for(int i=0;i< tdItems.size();i++)
            sum+=getFieldType(i).getLen();
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        String[]names=new String[td1.numFields()+ td2.numFields()];
        Type[] types=new Type[td2.numFields()+ td1.numFields()];
        for(int i=0;i< td1.numFields();i++) {
            names[i]= td1.getFieldName(i);
            types[i]=td1.getFieldType(i);
        }
        for(int i= td1.numFields();i< td1.numFields()+ td2.numFields();i++)
        {
            names[i]= td2.getFieldName(i- td1.numFields());
            types[i]=td2.getFieldType(i- td1.numFields());
        }
        return new TupleDesc(types,names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(o==null)
            return false;
        if(o.getClass()!=TupleDesc.class)
            return false;

        if(((TupleDesc)o).numFields()!=this.numFields())
            return false;
        for(int i=0;i<this.numFields();i++)
            if(!getFieldType(i).equals(((TupleDesc)o).getFieldType(i)))
                return false;
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.toString().hashCode();
        //throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String ret="";
        for(int i=0;i<numFields();i++)
        {
            ret+=getFieldType(i)+"["+i+"]"+"("+getFieldName(i)+")";
            if(i!=numFields()-1)
                ret+=",";
        }
        return "";
    }
}
