package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private int aField;
    private Type gbFieldType;
    private Op What;
    private TupleDesc tupleDesc;

    private Map<Field, Integer> map = new HashMap<>();
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
        if(what != Op.COUNT) throw new IllegalArgumentException("what != COUNT");
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.What = what;
        if(gbfield == NO_GROUPING){
            this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        }
        else{
            this.tupleDesc = new TupleDesc(new Type[] {gbFieldType, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field group;
        if(gbField == NO_GROUPING){
            group = null;
        }
        else{
            group = tup.getField(gbField);
        }
        Integer val = map.getOrDefault(group,0);
        map.put(group,val+1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> list = new ArrayList<>();
        for(Map.Entry<Field,Integer> entry: map.entrySet()){
            Field key = entry.getKey();
            Integer val = entry.getValue();
            Tuple t = new Tuple(tupleDesc);
            if(gbField == Aggregator.NO_GROUPING){
                t.setField(0, new IntField(val));
            }
            else{
                t.setField(0,key);
                t.setField(1,new IntField(val));
            }
            list.add(t);
        }
        return new TupleIterator(tupleDesc,list);

    }

}
