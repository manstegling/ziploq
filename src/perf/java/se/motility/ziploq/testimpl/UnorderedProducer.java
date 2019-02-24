package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.SynchronizedConsumer;

public class UnorderedProducer extends AbstractProducer {

    private final double messagesPerMilli;
    private final int maxDelay;

    public UnorderedProducer(SynchronizedConsumer<Object> consumer, int messages,
            double messagesPerMilli, int maxDelay, Runnable waitStrategy) {
        super(consumer, messages, waitStrategy);
        this.messagesPerMilli = messagesPerMilli;
        this.maxDelay = maxDelay;
    }

    @Override
    protected long getNextBusinessTs(int messageNo, long businessTs, long systemTs) {
        return START_TIME + (int) (messageNo / messagesPerMilli) - random.nextInt(maxDelay);
    }

    @Override
    protected long getNextSystemTs(int messageNo, long businessTs, long systemTs) {
        return START_TIME + (int) (messageNo / messagesPerMilli);
    }

    @Override
    protected boolean isEvent(int messageNo) {
        return true;
    }

}
