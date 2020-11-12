package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc tupleDesc;
    private Tuple tuple1;
    private Tuple tuple2;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        /*
        if (child1.hasNext()) {
            tuple1 = child1.next();
        }
        if (child2.hasNext()) {
            tuple2 = child2.next();
        }*/
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        tuple1 = null;
        tuple2 = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();

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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(child1.hasNext() && tuple1 == null){
            tuple1 = child1.next();
        }
        tuple2 = null;
        while (tuple1 != null) {
            //child2.rewind();
            while(child2.hasNext()){
                tuple2 = child2.next();

                Tuple mergedTuple = null;

                if (p.filter(tuple1, tuple2)) {
                    mergedTuple = mergeTuples(tuple1, tuple2);
                }
                if (mergedTuple != null) {
                    return mergedTuple;
                }
            }
            if(child1.hasNext()){
                tuple1 = child1.next();
                child2.rewind();
            }
            else break;
            /*
            if (child1.hasNext() && child2.hasNext()) {
                tuple1 = child1.next();
                tuple2 = child2.next();
            } else if (child1.hasNext()) {
                tuple1 = child1.next();
                child2.rewind();
                if (child2.hasNext()) {
                    tuple2 = child2.next();
                }
            } else if (child2.hasNext()) {
                tuple2 = child2.next();
                child1.rewind();
                if (child1.hasNext()) {
                    tuple1 = child1.next();
                }
            } else {
                tuple1 = null;
                tuple2 = null;
            }
             */



        }
        return null;
    }

    private Tuple mergeTuples(Tuple t1, Tuple t2) {
        Tuple mergedT = new Tuple(this.getTupleDesc());
        int size1 = t1.getTupleDesc().numFields();
        int size2 = t2.getTupleDesc().numFields();
        if (size1 + size2 != mergedT.getTupleDesc().numFields()) {
            return null;
        }
        int i = 0;
        while (i < size1) {
            mergedT.setField(i, t1.getField(i));
            i++;
        }
        while (i < size1 + size2) {
            mergedT.setField(i, t2.getField(i - size1));
            i++;
        }
        return mergedT;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1= children[0];
        child2 = children[1];
    }

}
