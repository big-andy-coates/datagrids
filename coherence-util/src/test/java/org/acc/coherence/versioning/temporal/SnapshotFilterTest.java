package org.acc.coherence.versioning.temporal;

import com.google.common.collect.ImmutableMap;
import com.oracle.common.collections.SubSet;
import com.oracle.common.collections.WrapperCollections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class SnapshotFilterTest {
    private final static int SNAPSHOT = 10;

    private TemporalExtractor extractor;
    private SnapshotFilter filter;
    private TemporalIndex index;
    private TimeLine timeLineA;
    private TimeLine timeLineB;

    @BeforeMethod
    public void setUp() {
        extractor = mock(TemporalExtractor.class);
        index = mock(TemporalIndex.class);

        filter = new SnapshotFilter(extractor, SNAPSHOT);
        timeLineA = mock(TimeLine.class, "timeline-A");
        timeLineB = mock(TimeLine.class, "timeline-B");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowOnNullExtractor() throws Exception {
        new SnapshotFilter(null, SNAPSHOT);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowOnNullSnapshot() throws Exception {
        new SnapshotFilter(extractor, null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowIfNoIndexMatchingExtractor() throws Exception {
        filter.applyIndex(Collections.emptyMap(), Collections.emptySet());
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowIfNotTemporalIndex() throws Exception {
        filter.applyIndex(ImmutableMap.of(extractor, "not what its expecting"), Collections.emptySet());
    }

    @Test
    public void shouldFilterEachKey() throws Exception {
        // Given:
        when(index.getTimeLine("keyA-1")).thenReturn(timeLineA);
        when(index.getTimeLine("keyB-1")).thenReturn(timeLineB);

        // When:
        filter.applyIndex(ImmutableMap.of(extractor, index), setWith("keyA-1", "keyB-1"));

        // Then:
        verify(index).getTimeLine("keyA-1");
        verify(index).getTimeLine("keyB-1");
    }

    @Test
    public void shouldAddFloorsIfNotNull() throws Exception {
        // Given:
        when(index.getTimeLine("keyA-1")).thenReturn(timeLineA);
        when(index.getTimeLine("keyA-2")).thenReturn(timeLineA);
        when(index.getTimeLine("keyB-1")).thenReturn(timeLineB);
        when(timeLineA.keySet()).thenReturn(setWith("keyA-1", "keyA-2"));
        when(timeLineB.keySet()).thenReturn(setWith("keyB-1"));
        when(timeLineA.get(SNAPSHOT)).thenReturn("keyA-2");
        when(timeLineB.get(SNAPSHOT)).thenReturn(null);
        Set<Object> keys = setWith("keyA-1", "keyA-2", "keyB-1");

        // When:
        filter.applyIndex(ImmutableMap.of(extractor, index), keys);

        // Then:
        assertThat(keys, contains((Object) "keyA-2"));
    }

    @Test
    public void shouldBeSmartAndNotGetTheSameTimeLineTwice() throws Exception {
        // Given:
        when(index.getTimeLine("keyA-1")).thenReturn(timeLineA);
        when(index.getTimeLine("keyB-1")).thenReturn(timeLineB);
        when(index.getTimeLine("keyB-2")).thenReturn(timeLineB);
        when(timeLineB.keySet()).thenReturn(setWith("keyB-1", "keyB-2"));
        Set<Object> keys = setWith("keyA-1", "keyB-1", "keyB-2");

        // When:
        filter.applyIndex(ImmutableMap.of(extractor, index), keys);

        // Then:
        verify(index, times(2)).getTimeLine(anyObject());
    }

    private static Set<Object> setWith(Object... values) {
        Set<Object> set = new HashSet<Object>();
        for (Object v : values) {
            set.add(v);
        }
        return new SubSet<Object>(set);
    }

}