package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.SynchronizedConsumer;

public class QuietOrderedProducer extends AbstractProducer {
    
    private final double fractionQuiet;

    public QuietOrderedProducer(SynchronizedConsumer<Object> consumer,
            int messages, double fractionQuiet, Runnable waitStrategy) {
        super(consumer, messages, waitStrategy);
        this.fractionQuiet = fractionQuiet;
    }

    @Override
    protected long getNextBusinessTs(int messageNo, long businessTs, long systemTs) {
        return businessTs + 1;
    }

    @Override
    protected long getNextSystemTs(int messageNo, long businessTs, long systemTs) {
        return businessTs;
    }

    @Override
    protected boolean isEvent(int messageNo) {
        return random.nextDouble() > fractionQuiet;
    }

}
