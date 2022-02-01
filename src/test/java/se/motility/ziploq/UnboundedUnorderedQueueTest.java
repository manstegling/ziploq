package se.motility.ziploq;

import se.motility.ziploq.api.BackPressureStrategy;

import static org.junit.Assert.*;

public class UnboundedUnorderedQueueTest extends AbstractUnorderedQueueTest {

    @Override
    BackPressureStrategy getStrategy() {
        return BackPressureStrategy.UNBOUNDED;
    }

}
