package org.acc.coherence.test;

import com.google.common.primitives.Ints;
import com.tangosol.net.CacheFactory;
import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.ClusterMemberGroupUtils;
import org.testng.annotations.*;

import java.util.*;

/**
 * Created by a.coates on 28/06/2014.
 */
public abstract class ClusterBasedTest {
    private static final int DEFAULT_STORAGE_NODE_COUNT = 2;

    private static ClusterMemberGroup memberGroup;
    private static Config runningConfig = new Config("coherence-cache-config.xml");
    private static final List<Integer> storageMemberIds = new ArrayList<Integer>();
    private static Map<Object, Object> additionalProperties = new HashMap<Object, Object>();

    private final Config requiredConfig;

    protected ClusterBasedTest(String clusterConfig) {
        this.requiredConfig = new Config(clusterConfig);
    }

    @AfterSuite
    public static void afterAllTests() {
        shutdownCluster();
    }

    @BeforeClass
    public void beforeClass() {
        ensureCluster(requiredConfig);
        restorePropertyChanges();
    }

    @BeforeMethod
    public void setUp() {
        clearCluster();
    }

    @AfterMethod
    public void tearDown() {
        ensureCorrectMemberCounts();
    }

    protected void clearCluster() {
        // Todo(ac):
    }

    protected void clearCaches(String... names) {
        for (String name : names) {
            CacheFactory.getCache(name).clear();
        }
    }

    protected void givenAllPartitionsHaveMoved() {
        List<Integer> storageNodes = getStorageMemberIds();
        addAdditionalStorageMembers(storageNodes.size());
        shutdownMembers(storageNodes);
    }

    protected List<Integer> getStorageMemberIds() {
        return new ArrayList<Integer>(storageMemberIds);
    }

    protected static void addAdditionalStorageMembers(int count) {
        ClusterMemberGroup newNode = ClusterMemberGroupUtils.newBuilder()
                .setStorageEnabledCount(count)
                .setCacheConfiguration(runningConfig.getClusterConfig())
                .setFastStartJoinTimeoutMilliseconds(1000)
                .buildAndConfigureForNoClient();

        storageMemberIds.addAll(Ints.asList(newNode.getStartedMemberIds()));

        memberGroup.merge(newNode);
    }

    protected void stopMembers(List<Integer> memberIds) {
        memberGroup.stopMember(Ints.toArray(memberIds));

        storageMemberIds.removeAll(memberIds);
    }

    protected void shutdownMembers(List<Integer> memberIds) {
        memberGroup.shutdownMember(Ints.toArray(memberIds));

        storageMemberIds.removeAll(memberIds);
    }

    private void ensureCorrectMemberCounts() {
        if (storageMemberIds.size() > DEFAULT_STORAGE_NODE_COUNT) {
            shutdownMembers(storageMemberIds.subList(DEFAULT_STORAGE_NODE_COUNT, storageMemberIds.size()));
        } else if (storageMemberIds.size() < DEFAULT_STORAGE_NODE_COUNT) {
            addAdditionalStorageMembers(DEFAULT_STORAGE_NODE_COUNT - storageMemberIds.size());
        }
    }

    private static void ensureCluster(Config newConfig) {
        if (!newConfig.equals(runningConfig)) {
            shutdownCluster();
            runningConfig = newConfig;
        }

        ensureCluster();
    }

    private static void ensureCluster() {
        if (memberGroup != null) {
            return;
        }

        final Properties preProps = snapshotProperties();

        memberGroup = ClusterMemberGroupUtils.newBuilder()
                .setStorageEnabledCount(0)
                .setCacheConfiguration(runningConfig.getClusterConfig())
                .setFastStartJoinTimeoutMilliseconds(100)
                .buildAndConfigureForStorageDisabledClient();

        storePropertyChanges(preProps);

        addAdditionalStorageMembers(DEFAULT_STORAGE_NODE_COUNT);
    }

    private static Properties snapshotProperties() {
        Properties snapshot = new Properties();
        snapshot.putAll(System.getProperties());
        return snapshot;
    }

    private static void storePropertyChanges(Properties preProps) {
        for (Map.Entry<Object, Object> currentProperty : System.getProperties().entrySet()) {
            final Object previousValue = preProps.get(currentProperty.getKey());
            if (previousValue != null && previousValue.equals(currentProperty.getValue())) {
                continue;
            }

            additionalProperties.put(currentProperty.getKey(), currentProperty.getValue());
        }
    }

    private static void restorePropertyChanges() {
        for (Map.Entry<Object, Object> property : additionalProperties.entrySet()) {
            System.getProperties().put(property.getKey(), property.getValue());
        }
    }

    private static void shutdownCluster() {
        if (memberGroup == null) {
            return;
        }

        if (ClusterMemberGroupUtils.shutdownCacheFactoryThenClusterMemberGroups(memberGroup)) {
            CacheFactory.shutdown();
        }
        memberGroup = null;
        storageMemberIds.clear();
        additionalProperties.clear();
    }

    private static class Config {
        private final String clusterConfig;

        public Config(String clusterConfig) {
            this.clusterConfig = clusterConfig;
        }

        public String getClusterConfig() {
            return clusterConfig;
        }
    }
}
