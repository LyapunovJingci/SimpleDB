package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private int numFields;
    private TDItem[] tdItem;

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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new Iterator<TDItem>(){

            private int loc = -1;

            public boolean hasNext() {
                return loc + 1 < tdItem.length;
            }

            public TDItem next() {
                if (hasNext()) {
                    loc++;
                    return tdItem[loc];
                }
                throw new NoSuchElementException();
            }
        };
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
        // Done
        if (typeAr.length < 1) throw new IllegalArgumentException("It must contain at least one entry.");
        if (typeAr.length != fieldAr.length) throw new IllegalArgumentException("should be same length");

        numFields = typeAr.length;
        tdItem = new TDItem[numFields];
        for (int i = 0; i < numFields; i++) {
            tdItem[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
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
        // Done
        if (typeAr.length < 1) throw new IllegalArgumentException("It must contain at least one entry.");

        numFields = typeAr.length;
        tdItem = new TDItem[numFields];
        this.numFields = typeAr.length;
        for (int i = 0; i < this.numFields; i++) {
            tdItem[i] = new TDItem(typeAr[i], "");
        }
    }
    /**
     * Constructor. Create a new tuple desc with TdItem
     *
     * @param TDItem
     *            copy value
     */
    public TupleDesc(TDItem[] TDItem) {
        // Done
        this.numFields = TDItem.length;
        tdItem = TDItem;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // Done
        return this.numFields;
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
        // Done
        if (i >= numFields || i < 0) throw new NoSuchElementException();
        return tdItem[i].fieldName;
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
        // Done
        if (i >= numFields || i < 0) throw new NoSuchElementException();
        return tdItem[i].fieldType;
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
        // Done
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < numFields; i++){
            if (tdItem[i].fieldName != null && tdItem[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // Done
        int count = 0;
        for(TDItem item : tdItem){
            count += item.fieldType.getLen();
        }
        return count;
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
        // Done
        TDItem[] tdi1 = td1.tdItem;
        TDItem[] tdi2 = td2.tdItem;
        TDItem[] res = new TDItem[tdi1.length + tdi2.length];
        for(int i = 0; i < tdi1.length + tdi2.length; i++){
            if (i < tdi1.length) {
                res[i] = tdi1[i];
            } else {
                res[i] = tdi2[i - tdi1.length];
            }
        }
        return new TupleDesc(res);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o){
        // Done
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        TupleDesc tmp = (TupleDesc)o;

        if (tmp.getSize() != this.getSize() || tmp.numFields() != this.numFields()) {
            return false;
        }

        for (int i = 0; i < this.numFields() - 1; i++){
            if (!this.tdItem[i].equals(((TupleDesc) o).tdItem[i])){
                return false;
            }
        }
        return true;

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // Done
        StringBuffer strBuffer = new StringBuffer();
        for (TDItem item : tdItem) {
            strBuffer.append(item.fieldType);
            strBuffer.append("(").append(item.fieldName).append(")").append(",");
        }
        strBuffer.deleteCharAt(strBuffer.length() - 1);
        return strBuffer.toString();
    }
}
