package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int min;
    private int max;
    private int[] histo;
    private int bucketSize;
    private int lastBucketSize;
    private int count;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        if (buckets >= max - min + 1) {
            this.buckets = max - min + 1;
            this.histo = new int[buckets];
            this.bucketSize = 1;
            this.lastBucketSize = 1;
        } else {
            this.buckets = buckets;
            this.histo = new int[buckets];
            this.bucketSize = (int) Math.ceil((double)(max - min + 1) / (double)buckets);
            this.lastBucketSize = max - ((buckets - 1) * bucketSize + min) + 1;
        }
        this.count = 0;
    }

    private int getIndex(int value) {
        if (value == max) {
            return buckets - 1;
        }

        return (value - this.min) / this.bucketSize;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getIndex(v);
        histo[index] += 1;
        count+= 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        // special case check
        if (v < min) {
            switch (op) {
                case EQUALS:
                case LESS_THAN_OR_EQ:
                case LESS_THAN:
                    return 0;
                case NOT_EQUALS:
                case GREATER_THAN_OR_EQ:
                case GREATER_THAN:
                    return 1;

            }
        }

        if (v > max) {
            switch (op) {
                case EQUALS:
                case GREATER_THAN_OR_EQ:
                case GREATER_THAN:
                    return 0;
                case NOT_EQUALS:
                case LESS_THAN_OR_EQ:
                case LESS_THAN:
                    return 1;
            }
        }

        //Normal case

        int bucketIndex = getIndex(v);
        int width = bucketIndex == buckets - 1 ? lastBucketSize : bucketSize;
        int height = histo[bucketIndex];
        int ntups = count;
        switch (op) {
            case EQUALS:
                return ((double) height / (double) width) / (double) ntups;
            case NOT_EQUALS:
                return 1 - ((double) height / (double) width) / (double) ntups;
            case LESS_THAN_OR_EQ:
            case GREATER_THAN:
                double selectivityOfGreaterThan;
                if (bucketIndex == buckets - 1) {
                    // value at last bucket
                    selectivityOfGreaterThan = getSelectivityForRight(bucketIndex, v);
                } else {
                    selectivityOfGreaterThan = getSelectivityForRight(bucketIndex, v);
                    int i = bucketIndex + 1;
                    while (i < buckets) {
                        selectivityOfGreaterThan += getSelectivityForFull(i);
                        i++;
                    }
                }
                if (op.equals(Predicate.Op.GREATER_THAN)) {
                    return selectivityOfGreaterThan;
                } else {
                    //LESS_THAN_OR_EQ:
                    return 1 - selectivityOfGreaterThan;
                }

            case GREATER_THAN_OR_EQ:
            case LESS_THAN:
                double selectivityOfLessThan = 0;
                if (bucketIndex == 0) {
                    // value at last bucket
                    selectivityOfLessThan = getSelectivityForLeft(bucketIndex, v);
                } else {
                    selectivityOfLessThan = getSelectivityForLeft(bucketIndex, v);
                    int i = bucketIndex - 1;
                    while (i >= 0) {
                        selectivityOfLessThan += getSelectivityForFull(i);
                        i--;
                    }
                }

                if (op.equals(Predicate.Op.LESS_THAN)) {
                    return selectivityOfLessThan;
                } else {
                    //GREATER_THAN_OR_EQ
                    return 1 - selectivityOfLessThan;
                }

        }
        return 0;
    }
    private double getSelectivityForFull(int bucketIndex) {
        double b_f = (double) histo[bucketIndex] / count;
        double b_part = 1;
        return b_f * b_part;
    }
    private double getSelectivityForRight(int bucketIndex, int v) {
        int b_right = (bucketIndex + 1) * bucketSize + min - 1;
        double b_f = (double) histo[bucketIndex] / count;
        double b_part = (double) (b_right - v) / bucketSize;
        if (bucketIndex == buckets - 1) {
            b_part = (double) (b_right - v) / lastBucketSize;
        }
        return b_f * b_part;
    }
    private double getSelectivityForLeft(int bucketIndex, int v) {
        int b_left = bucketIndex * bucketSize + min;
        double b_f = (double) histo[bucketIndex] / count;
        double b_part = (double) (v - b_left) / bucketSize;
        if (bucketIndex == buckets - 1) {
            b_part = (double) (v - b_left) / lastBucketSize;
        }
        return b_f * b_part;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        builder.append("buckets = ");
        builder.append(buckets);
        builder.append( "min = ");
        builder.append(min);
        builder.append(" max = ");
        builder.append(max);
        return builder.toString();
    }
}
