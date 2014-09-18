package org.acc.coherence.test;

import com.google.common.primitives.Ints;
import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.ClusterMemberGroupUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by a.coates on 28/06/2014.
 */
public abstract class ClusterBasedTest {
    private static final int DEFAULT_STORAGE_NODE_COUNT = 2;

    private static ClusterMemberGroup memberGroup;
    private static Config runningConfig = new Config("coherence-cache-config.xml");
    private static final List<Integer> storageMemberIds = new ArrayList<Integer>();

    private final Config requiredConfig;

    protected ClusterBasedTest(String clusterConfig) {
        this.requiredConfig = new Config(clusterConfig);
    }

    @BeforeClass
    public void beforeTests() {
        ensureCluster(requiredConfig);
    }

    @AfterSuite
    public static void afterAllTests() {
        shutdownCluster();
    }

    @AfterMethod
    public void tearDown() {
        ensureCorrectMemberCounts();
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

        memberGroup = ClusterMemberGroupUtils.newBuilder()
                .setStorageEnabledCount(0)
                .setCacheConfiguration(runningConfig.getClusterConfig())
                .setFastStartJoinTimeoutMilliseconds(1000)
                .buildAndConfigureForStorageDisabledClient();

        addAdditionalStorageMembers(DEFAULT_STORAGE_NODE_COUNT);
    }

    private static void shutdownCluster() {
        if (memberGroup == null) {
            return;
        }

        ClusterMemberGroupUtils.shutdownCacheFactoryThenClusterMemberGroups(memberGroup);
        memberGroup = null;
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
