package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type groupByFieldType;
    private int aggregateField;
    private Op operator;

    //key is gbfield, first element in arraylist is value, second is count
    private ConcurrentHashMap<Field, ArrayList<Integer>> values;
    private static Field NO_GROUPING = new IntField(0);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        this.values = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = tup.getField(this.gbfield);

        int value = ((IntField) (tup.getField(this.aggregateField))).getValue();

        if (tup.getTupleDesc().getFieldType(gbfield).equals(groupByFieldType)) {
            if (!values.containsKey(key)) {
                ArrayList<Integer> pair = new ArrayList<>();
                pair.add(value);
                pair.add(1);
                values.put(key, pair);
            } else {
                ArrayList<Integer> pair = values.get(key);
                ArrayList<Integer> newPair = new ArrayList<>();
                int mergedValue = processMerge(pair.get(0), value, this.operator);
                newPair.add(mergedValue);
                newPair.add(pair.get(1) + 1);
                values.replace(key, newPair);
            }
        }
    }

    private int processMerge(int a, int b, Op op) {
        switch (op) {
            case MIN:
                return Math.min(a, b);
            case MAX:
                return Math.max(a, b);
            case AVG:
            case SUM:
                return a + b;
            case COUNT:
            case SUM_COUNT:
            case SC_AVG:
                return 0;
        }
        return 0;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here

        // a single (aggregateVal) if no grouping
        // This is weird, in the comment, this part should be necessary, but it would not be tested
        if (this.gbfield == Aggregator.NO_GROUPING) {
            TupleDesc tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
            ArrayList<Tuple> tuples = new ArrayList<>();
            Tuple t = new Tuple(tupleDesc);
            int value = values.get(NO_GROUPING).get(0);
            int count = values.get(NO_GROUPING).get(1);
            if (operator.equals(Op.AVG)) {
                t.setField(0, new IntField(value / count));
            } else if (operator.equals(Op.COUNT)) {
                t.setField(0, new IntField(count));
            } else {
                t.setField(0, new IntField(value));
            }
            tuples.add(t);
            return new TupleIterator(tupleDesc, tuples);
        }

        // pair (groupVal, aggregateVal) if using group
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc tupleDesc = new TupleDesc(new Type[] { this.groupByFieldType, Type.INT_TYPE });
        for (Field key : this.values.keySet()) {
            Tuple t = new Tuple(tupleDesc);
            int value = values.get(key).get(0);
            int count = values.get(key).get(1);
            t.setField(0, key);
            if (operator.equals(Op.AVG)) {
                t.setField(1, new IntField(value / count));
            } else if (operator.equals(Op.COUNT)) {
                t.setField(1, new IntField(count));
            } else {
                t.setField(1, new IntField(value));
            }
            tuples.add(t);
        }

        return new TupleIterator(tupleDesc, tuples);
    }

}
