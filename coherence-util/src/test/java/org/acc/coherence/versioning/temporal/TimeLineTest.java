package org.acc.coherence.versioning.temporal;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author Andy Coates.
 */
public class TimeLineTest {
    private TimeLine timeLine;

    @BeforeMethod
    public void setUp() {
        timeLine = new TimeLine();
    }

    @Test
    public void shouldInsertKey() throws Exception {
        // When:
        timeLine.insert("this", 1);

        // Then:
        assertThat(timeLine.keySet(), contains((Object) "this"));
    }

    @Test
    public void shouldRemoveKey() throws Exception {
        // Given:
        timeLine.insert("this", 1);

        // When:
        timeLine.remove("this", 1);

        // Then:
        assertThat(timeLine.keySet(), is(empty()));
    }

    @Test
    public void shouldReturnFalseOnRemoveIfNotPresent() throws Exception {
        // When:
        final boolean removed = timeLine.remove("this", 1);

        // Then:
        assertThat(removed, is(false));
    }

    @Test
    public void shouldReturnTrueOnRemoveIfPresent() throws Exception {
        // Given:
        timeLine.insert("this", 1);

        // When:
        final boolean removed = timeLine.remove("this", 1);

        // Then:
        assertThat(removed, is(true));
    }

    @Test
    public void shouldCorrectlyReportIfEmpty() throws Exception {
        // Given:
        timeLine.insert("bob", 12);

        // Then:
        assertThat(timeLine.isEmpty(), is(false));
        assertThat(new TimeLine().isEmpty(), is(true));
    }

    @Test
    public void shouldReturnToEmptyWhenLastKeyRemoved() throws Exception {
        // Given:
        timeLine.insert("bob", 12);

        // When:
        timeLine.remove("bob", 12);

        // Then:
        assertThat(timeLine.isEmpty(), is(true));
    }

    @Test
    public void shouldGetNullIfEmpty() throws Exception {
        assertThat(timeLine.get(1234), is(nullValue()));
    }

    @Test
    public void shouldGetNullIfBeforeFirstVersion() throws Exception {
        // Given:
        givenTimeLinePopulated();

        // When:
        Object key = timeLine.get(4);

        // Then:
        assertThat(key, is(nullValue()));
    }

    @Test
    public void shouldGetKeyIfOnKey() throws Exception {
        // Given:
        givenTimeLinePopulated();

        // When:
        Object key = timeLine.get(5);

        // Then:
        assertThat(key, is((Object) "key1"));
    }

    @Test
    public void shouldGetPreviousKeyIfBetweenKeys() throws Exception {
        // Given:
        givenTimeLinePopulated();

        // When:
        Object key = timeLine.get(9);

        // Then:
        assertThat(key, is((Object) "key1"));
    }

    @Test
    public void shouldGetLastKeyIfBeyondRange() throws Exception {
        // Given:
        givenTimeLinePopulated();

        // When:
        Object key = timeLine.get(100);

        // Then:
        assertThat(key, is((Object) "key6"));
    }

    @Test
    public void shouldGetHighestVersionWhenTwoKeysHaveSameTimestamp() throws Exception {
        // Given:
        givenTimeLinePopulated();

        // When:
        Object key = timeLine.get(11);

        // Then:
        assertThat(key, is((Object) "key3"));
    }

    @Test
    public void shouldGetHighestVersionWhenTwoKeysHaveSameTimestampRegardlessOfOrder() throws Exception {
        // Given:
        givenTimeLinePopulated();

        // When:
        Object key = timeLine.get(16);

        // Then:
        assertThat(key, is((Object) "key5"));
    }

    @Test
    public void shouldObeyComparitor() throws Exception {
        // Given:
        timeLine = new TimeLine(Comparator.reverseOrder());
        givenTimeLinePopulated();

        // When:
        Object key = timeLine.get(16);

        // Then:
        assertThat(key, is((Object) "key4"));
    }

    @Test
    public void shouldReturnKeys() throws Exception {
        // Given:
        givenTimeLinePopulated();

        // Then:
        assertThat(timeLine.keySet(), containsInAnyOrder(
                (Object) "key1", "key2", "key3", "key4", "key5", "key6"
        ));
    }


    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowOnDuplicateKey() throws Exception {
        // Given:
        timeLine.insert("key1", 5);

        // When:
        timeLine.insert("key1", 9);
    }

    private void givenTimeLinePopulated() {
        timeLine.insert("key1", 5);
        timeLine.insert("key2", 10);
        timeLine.insert("key3", 10);
        timeLine.insert("key5", 15);
        timeLine.insert("key4", 15);
        timeLine.insert("key6", 20);
    }
}