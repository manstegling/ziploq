package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.SynchronizedConsumer;

public class UnorderedProducerNoSystemTime extends UnorderedProducer {

    public UnorderedProducerNoSystemTime(SynchronizedConsumer<Object> consumer, int messages, double messagesPerMilli,
            int maxDelay, Runnable waitStrategy) {
        super(consumer, messages, messagesPerMilli, maxDelay, waitStrategy);
    }

    @Override
    protected long getNextSystemTs(int messageNo, long businessTs, long systemTs) {
        return Long.MAX_VALUE;
    }

}
