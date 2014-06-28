package org.acc.coherence.test;

import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.ClusterMemberGroupUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Created by andy on 28/06/2014.
 */
public abstract class ClusterBasedTest {
    private static ClusterMemberGroup memberGroup;
    private static Config runningConfig = new Config("coherence-cache-config.xml");

    protected ClusterBasedTest(String clusterConfig) {
        runningConfig = new Config(clusterConfig);
    }

    @BeforeClass
    public static void beforeTests() {
        memberGroup = ClusterMemberGroupUtils.newBuilder()
                .setStorageEnabledCount(2)
                .setCacheConfiguration(runningConfig.getClusterConfig())
                        //.setOverrideConfiguration()
                .buildAndConfigureForStorageDisabledClient();
    }

    @AfterClass
    public static void afterTests() {
        ClusterMemberGroupUtils.shutdownCacheFactoryThenClusterMemberGroups(memberGroup);
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
