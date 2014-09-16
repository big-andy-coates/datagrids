package org.acc.coherence.versioning.temporal;

import com.tangosol.io.Serializer;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.CacheService;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.MapTrigger;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.extractor.AbstractExtractor;
import org.acc.coherence.test.TestBinaryEntry;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

public class TemporalIndexTest {
    private static final Serializer serialiser =
            new ConfigurablePofContext("org/acc/coherence/versioning/temporal/temporal-versioning-pof-config.xml");
    private TemporalIndex index;
    private TemporalExtractor temporalExtactor;
    private BackingMapContext context;
    private AbstractExtractor versionExtractor;
    private AbstractExtractor businessKeyExtractor;
    private AbstractExtractor timestampExtractor;

    @BeforeMethod
    public void setUp() {
        context = mockSerialiserChain();
        temporalExtactor = mock(TemporalExtractor.class);
        versionExtractor = mock(AbstractExtractor.class, "versionExtractor");
        businessKeyExtractor = mock(AbstractExtractor.class, "businessKeyExtractor");
        timestampExtractor = mock(AbstractExtractor.class, "timestampExtractor");

        when(temporalExtactor.getBusinessKeyExtractor()).thenReturn(businessKeyExtractor);
        when(temporalExtactor.getTimestampExtractor()).thenReturn(timestampExtractor);
        when(temporalExtactor.getVersionExtractor()).thenReturn(versionExtractor);

        index = new TemporalIndex(temporalExtactor, context);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowOnNullExtractor() throws Exception {
        new TemporalIndex(null, context);
    }

    @Test
    public void shouldReturnExtractor() {
        assertThat(index.getValueExtractor(), is((ValueExtractor) temporalExtactor));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowFromIsPartial() throws Exception {
        index.isPartial();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowFromIsOrdered() throws Exception {
        index.isOrdered();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowFromGetIndexContents() throws Exception {
        index.getIndexContents();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowFromGet() throws Exception {
        index.get(new Object());
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowFromGetComparator() throws Exception {
        index.getComparator();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldThrowFromUpdate() throws Exception {
        Map.Entry entry = mock(Map.Entry.class);
        index.update(entry);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldThrowFromDeleteOnUnknownBusinessKey() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        Map.Entry entry = createTestEntry("unknownKey", 1123, 3L);

        // When:
        index.delete(createdDeletedTestEntry(entry));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIfCantExtractBusinessKeyFromEntryOnInsert() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        Map.Entry entry = createTestEntry(null, 1123, 3L);

        // When:
        index.insert(entry);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIfCantExtractBusinessKeyFromEntryOnDelete() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        Map.Entry entry = createTestEntry(null, 1123, 3L);

        // When:
        index.delete(createdDeletedTestEntry(entry));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIfCantExtractTimestampFromEntryOnInsert() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        Map.Entry entry = createTestEntry("someKey", 12, null);

        // When:
        index.insert(entry);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIfCantExtractTimestampFromEntryOnDelete() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        Map.Entry entry = createTestEntry("someKey", 12, null);
        givenTimeLineExistsFor("someKey");

        // When:
        index.delete(createdDeletedTestEntry(entry));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIfCantExtractVersionFromEntryOnInsert() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        Map.Entry entry = createTestEntry("someKey", null, 12345L);

        // When:
        index.insert(entry);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldThrowIfUnknownTimeLineRequests() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        BinaryEntry entry = createTestEntry("businessKey", 1, 11234L);

        // When:
        index.getTimeLine(entry.getBinaryKey());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldThrowIfUnknownTimestampWhenDeletingEntry() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        givenTimeLineExistsFor("businessKey");
        BinaryEntry entry = createdDeletedTestEntry(createTestEntry("businessKey", 1, 11234L));

         // When:
        index.delete(entry);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldThrowIfUnknownBusinessKeyWhenDeletingEntry() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        BinaryEntry entry = createdDeletedTestEntry(createTestEntry("businessKey", 1, 11234L));

        // When:
        index.delete(entry);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIfItCantExtractBusinessKeyWhenRetrievingTimeLine() throws Exception {
        // Given:
        BinaryEntry entry = createTestEntry(null, 1, 11234L);

        // When:
        index.getTimeLine(entry.getBinaryKey());
    }

    @Test
    public void shouldCreateNewTimeLineOnFirstOccurrenceOfBusinessKey() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        BinaryEntry entry = createTestEntry("businessKey", 1, 11234L);

        // When:
        index.insert(entry);

        // Then:
        TimeLine timeLine = index.getTimeLine(entry.getBinaryKey());
        assertThat(timeLine, is(notNullValue()));
        assertThat(timeLine.keySet(), contains((Object) entry.getBinaryKey()));
        assertThat(timeLine.get(11234L), is((Object)entry.getBinaryKey()));
    }

    @Test
    public void shouldAddSecondEntryToTimeLine() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        TestBinaryEntry entry1 = createTestEntry("businessKey", 1, 11234L);
        index.insert(entry1);
        TestBinaryEntry entry2 = createTestEntry("businessKey", 2, 11235L);

        // When:
        index.insert(entry2);

        // Then:
        TimeLine timeLine = index.getTimeLine(entry2.getBinaryKey());
        assertThat(timeLine, is(notNullValue()));
        assertThat(timeLine.keySet(), containsInAnyOrder((Object) entry1.getBinaryKey(), entry2.getBinaryKey()));
        assertThat(timeLine.get(11234L), is((Object)entry1.getBinaryKey()));
        assertThat(timeLine.get(11235L), is((Object)entry2.getBinaryKey()));
    }

    @Test
    public void shouldKeepDifferentBusinessKeysInDifferentTimeLines() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        TestBinaryEntry entry1 = createTestEntry("businessKey1", 1, 11234L);
        index.insert(entry1);
        TestBinaryEntry entry2 = createTestEntry("businessKey2", 2, 11235L);

        // When:
        index.insert(entry2);

        // Then:
        assertThat(index.getTimeLine(entry1.getBinaryKey()).keySet(), hasSize(1));
        assertThat(index.getTimeLine(entry2.getBinaryKey()).keySet(), hasSize(1));
    }

    @Test
    public void shouldRemoveTimeLineWhenLastVersionIsRemoved() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        TestBinaryEntry entry = createTestEntry("businessKey", 1, 11234L);
        index.insert(entry);

        // When:
        index.delete(createdDeletedTestEntry(entry));

        // Then:
        try {
            index.getTimeLine(entry.getBinaryKey());
            fail("timeline should be unknown");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIfExtractedVersionNotComparable() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        givenVersionExtractorSetUpToReturnNonComparable();
        TestBinaryEntry entry = createTestEntry("businessKey", 1, 11234L);

        // When:
        index.insert(entry);
    }

    @Test
    public void shouldWorkWithNaturalOrderingOfKeys() throws Exception {
        // Given:
        givenExtractorsSetUpToUseTestBinary();
        givenUsingNaturalOrderingOfKeys();
        TestBinaryEntry entry1 = createTestEntry("businessKey", 1, 11234L);
        index.insert(entry1);
        TestBinaryEntry entry2 = createTestEntry("businessKey", 2, 11235L);

        // When:
        index.insert(entry2);

        // Then:
        TimeLine timeLine = index.getTimeLine(entry2.getBinaryKey());
        assertThat(timeLine, is(notNullValue()));
        assertThat(timeLine.keySet(), containsInAnyOrder((Object) entry1.getBinaryKey(), entry2.getBinaryKey()));
        assertThat(timeLine.get(11234L), is((Object)entry1.getBinaryKey()));
        assertThat(timeLine.get(11235L), is((Object)entry2.getBinaryKey()));
    }

    private static TestBinaryEntry createTestEntry(String businessKey, Integer version, Long timestamp) {
        return new TestBinaryEntry(businessKey + "-v" + version, "value-ts" + timestamp, serialiser);
    }

    private static TestBinaryEntry createdDeletedTestEntry(Map.Entry addedEntry) {
        return new TestBinaryEntry(addedEntry.getKey(), null, addedEntry.getValue(), serialiser);
    }

    private static BackingMapContext mockSerialiserChain() {
        BackingMapContext context = mock(BackingMapContext.class);
        BackingMapManagerContext managerContext = mock(BackingMapManagerContext.class);
        CacheService service = mock(CacheService.class);
        when(context.getManagerContext()).thenReturn(managerContext);
        when(managerContext.getCacheService()).thenReturn(service);
        when(service.getSerializer()).thenReturn(serialiser);
        return context;
    }

    private void givenExtractorsSetUpToUseTestBinary() {
        when(businessKeyExtractor.extractFromEntry(any(Map.Entry.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Map.Entry entry = (Map.Entry) invocation.getArguments()[0];
                String key = (String) entry.getKey();
                String businessKey = key.substring(0, key.indexOf("-v"));
                return "null".equals(businessKey) ? null : businessKey;
            }
        });
        when(businessKeyExtractor.extract(anyObject())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                String key = (String) invocation.getArguments()[0];
                String businessKey = key.substring(0, key.indexOf("-v"));
                return "null".equals(businessKey) ? null : businessKey;
            }
        });

        when(timestampExtractor.extractFromEntry(any(Map.Entry.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Map.Entry entry = (Map.Entry) invocation.getArguments()[0];
                String value = (String) entry.getValue();
                String timestamp = value.substring(value.indexOf("-ts") + 3, value.length());
                return "null".equals(timestamp) ? null : Long.parseLong(timestamp);
            }
        });
        when(timestampExtractor.extractOriginalFromEntry(any(MapTrigger.Entry.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                MapTrigger.Entry entry = (MapTrigger.Entry) invocation.getArguments()[0];
                String value = (String) entry.getOriginalValue();
                String timestamp = value.substring(value.indexOf("-ts") + 3, value.length());
                return "null".equals(timestamp) ? null : Long.parseLong(timestamp);
            }
        });

        when(versionExtractor.extract(anyObject())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                String key = (String) invocation.getArguments()[0];
                String version = key.substring(key.indexOf("-v") + 2, key.length());
                return "null".equals(version) ? null : Integer.parseInt(version);
            }
        });
    }

    private void givenVersionExtractorSetUpToReturnNonComparable() {
        versionExtractor = mock(AbstractExtractor.class, "versionExtractor");

        when(versionExtractor.extract(anyObject())).thenReturn(new Object());

        when(temporalExtactor.getVersionExtractor()).thenReturn(versionExtractor);
        index = new TemporalIndex(temporalExtactor, context);
    }

    private void givenUsingNaturalOrderingOfKeys() {
        versionExtractor = null;

        when(temporalExtactor.getVersionExtractor()).thenReturn(versionExtractor);
        index = new TemporalIndex(temporalExtactor, context);
    }

    private void givenTimeLineExistsFor(String key) {
        // Requires givenExtractorsSetUpToUseTestBinary() to have been called...
        BinaryEntry entry = new TestBinaryEntry(key + "-v101", "value-ts9999", serialiser);
        index.insert(entry);
    }
}
