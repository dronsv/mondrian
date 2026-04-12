package mondrian.bench;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Seed benchmark: measures instanceof+cast vs instanceof pattern match
 * dispatch. Used to validate JMH setup and calibrate expectations.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Thread)
public class InstanceOfDispatchBenchmark {

    private Object value;

    @Setup
    public void setup() {
        value = Double.valueOf(42.0);
    }

    @Benchmark
    public double oldStyle() {
        if (value instanceof Number) {
            Number n = (Number) value;
            return n.doubleValue();
        }
        return 0.0;
    }

    @Benchmark
    public double patternMatch() {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return 0.0;
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(InstanceOfDispatchBenchmark.class.getSimpleName())
            .build();
        new Runner(opt).run();
    }
}
