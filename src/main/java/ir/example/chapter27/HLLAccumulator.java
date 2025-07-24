package ir.example.chapter27;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import java.io.Serializable;
import org.apache.spark.util.AccumulatorV2;

/**
 * An {@link AccumulatorV2 accumulator} for counting unique elements using a HyperLogLog
 */
public class HLLAccumulator<T> extends AccumulatorV2<T, Long> implements Serializable {

    private final int precisionValue;
    private HyperLogLogPlus hll;

    public HLLAccumulator() {
        this(12);
    }

    public HLLAccumulator(int precisionValue) {
        if (precisionValue < 4 || precisionValue > 32) {
            throw new IllegalArgumentException("precision value must be between 4 and 32");
        }
        this.precisionValue = precisionValue;
        this.hll = instance();
    }

    private HyperLogLogPlus instance() {
        return new HyperLogLogPlus(precisionValue, 0);
    }

    @Override
    public boolean isZero() {
        System.out.println("size " + hll.cardinality());
        return hll.cardinality() == 0;
    }

    @Override
    public AccumulatorV2<T, Long> copyAndReset() {
        return new HLLAccumulator<>(precisionValue);
    }

    @Override
    public HLLAccumulator<T> copy() {
        HLLAccumulator<T> newAcc = new HLLAccumulator<>(precisionValue);
        try {
            newAcc.hll.addAll(this.hll);
        } catch (CardinalityMergeException e) {
            throw new RuntimeException("Unable to add all for new hyperloglog instance", e);
        }
        return newAcc;
    }

    @Override
    public void reset() {
        this.hll = instance();
    }

    @Override
    public void add(T v) {
        hll.offer(v);
    }

    @Override
    public void merge(AccumulatorV2<T, Long> other) {
        if (other instanceof HLLAccumulator<T> otherHllAcc) {
            try {
                hll.addAll(otherHllAcc.hll);
            } catch (CardinalityMergeException e) {
                throw new RuntimeException("Unable to add all for new hyperloglog instance", e);
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Cannot merge %s with %s",
                            this.getClass().getName(),
                            other.getClass().getName()));
        }
    }

    @Override
    public Long value() {
        return hll.cardinality();
    }
}