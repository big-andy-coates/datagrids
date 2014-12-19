package org.acc.coherence.test;

import com.tangosol.util.MapEvent;
import com.tangosol.util.MultiplexingMapListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author datalorax - 14/11/2014.
 */
public class CountdownMapListener extends MultiplexingMapListener {
    private final CountDownLatch latch;

    public CountdownMapListener(int count) {
        this.latch = new CountDownLatch(count);
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    @Override
    protected void onMapEvent(MapEvent mapEvent) {
        latch.countDown();
    }

    public int getRemaining() {
        return (int)latch.getCount();
    }
}
