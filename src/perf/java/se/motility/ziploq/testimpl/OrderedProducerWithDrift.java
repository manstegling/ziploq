package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.FlowConsumer;

public class OrderedProducerWithDrift extends AbstractProducer {
    
    private static final int INCREMENT = 2;
    
    private final int maxDrift;
    
    private boolean incrementBusinessTs;
    
    public OrderedProducerWithDrift(FlowConsumer<Object> consumer,
            int messages, int maxDrift, Runnable waitStrategy) {
        super(consumer, messages, waitStrategy);
        this.maxDrift = maxDrift;
    }

    @Override
    protected long getNextBusinessTs(int messageNo, long businessTs, long systemTs) {
        //random walk with hard boundaries
        long delta = businessTs - systemTs;
        double threshold;
        if (delta <= -maxDrift + INCREMENT) {
            threshold = 0d;
        } else if (delta >= maxDrift - INCREMENT) {
            threshold = 1d;
        } else {
            threshold = 0.5d;
        }
        incrementBusinessTs = random.nextDouble() > threshold;
        return incrementBusinessTs ? businessTs + INCREMENT : businessTs;
    }
    
    @Override
    protected long getNextSystemTs(int messageNo, long businessTs, long systemTs) {
        return incrementBusinessTs ? systemTs : systemTs + INCREMENT;
    }

    @Override
    protected boolean isEvent(int messageNo) {
        return true;
    }
    
    
}
