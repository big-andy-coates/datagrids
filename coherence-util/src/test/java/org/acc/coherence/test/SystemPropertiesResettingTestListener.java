package org.acc.coherence.test;

import org.testng.*;

import java.io.FileNotFoundException;
import java.lang.management.ManagementFactory;
import java.util.Properties;

/**
 * Resets system properties before each test class, to stop properties from one class bleeding into another.
 * @author data lorax
 */
public class SystemPropertiesResettingTestListener implements IInvokedMethodListener {

    static {
        ManagementFactory.getPlatformMBeanServer();
    }

    private final Properties preSuiteProps = new Properties();
    private Class<?> currentClass = Object.class;

    public SystemPropertiesResettingTestListener() throws FileNotFoundException {
        savePropertiesBeforeAnyTestConstructorsRun();
    }

    @Override
    public void beforeInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
        final ITestNGMethod testMethod = iInvokedMethod.getTestMethod();

        if (testMethod.isBeforeTestConfiguration()) {
            return;
        }

        if (!testMethod.isBeforeClassConfiguration() && !testMethod.isBeforeMethodConfiguration() && !testMethod.isTest()) {
            return;
        }

        final Class testClass = testMethod.getTestClass().getRealClass();
        if (testClassHasChanged(testClass)) {
            currentClass = testClass;
            restorePreSuiteProperties();
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
    }

    private void restorePreSuiteProperties() {
        Properties properties = System.getProperties();
        properties.keySet().retainAll(preSuiteProps.keySet());
        properties.putAll(preSuiteProps);
    }

    private void savePropertiesBeforeAnyTestConstructorsRun() {
        preSuiteProps.clear();
        preSuiteProps.putAll(System.getProperties());
    }

    private boolean testClassHasChanged(Class testClass) {
        return !classesEqual(testClass, currentClass);
    }

    public static boolean classesEqual(Class o1, Class o2) {
        if (o1 == o2) {
            return true;
        }

        if ((o1 == null) || (o2 == null)) {
            return false;
        }

        try {
            return o1.equals(o2);
        } catch (RuntimeException e) {
            return false;
        }
    }
}

