package extension.util;

import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Optional;

public class ResourcesPathUtil {
    public static final String PROJECT_RESOURCE_PATH = "src/test/resources/";
    public static final String TEST_SUFFIX = "Test";

    public static String filePathToClassPath(String filePath) {
        return filePath.substring(PROJECT_RESOURCE_PATH.length());
    }

    /**
     * stubs/ClassName/methodName
     */
    public static String getStubClassPath(Method testMethod) {
        String methodName = testMethod.getName();
        String className = testMethod.getDeclaringClass().getSimpleName();
        String classStubFolder = className.endsWith(TEST_SUFFIX) ? trimEnd(className, TEST_SUFFIX) : className;

        return "stubs/" + classStubFolder + "/" + methodName;
    }

    public static String testClassNameToStubClassPath(String testClassName) {
        String classStubFolder = testClassName.endsWith(TEST_SUFFIX) ? trimEnd(testClassName, TEST_SUFFIX) : testClassName;
        return "stubs/" + classStubFolder;
    }

    /**
     * src/test/resources/stubs/ClassName/methodName
     */
    public static String getStubFilePath(Method testMethod) {
        return classPathToFilePath(getStubClassPath(testMethod));
    }

    public static String testClassNameToStubFolder(String testClassName) {
        return classPathToFilePath(testClassNameToStubClassPath(testClassName));
    }

    public static String classPathToFilePath(String classPath) {
        return PROJECT_RESOURCE_PATH + classPath;
    }

    public static String trimEnd(String original, String ending) {
        return original.substring(0, original.length() - ending.length());
    }

    public static String getStubClassPathOfTestClass(ExtensionContext extensionContext) {
        Optional<Class<?>> optionalClass = extensionContext.getTestClass();

        if (optionalClass.isEmpty()) {
            throw new IllegalArgumentException("No test class available in the extension context");
        }

        String testClassName = optionalClass.get().getSimpleName();
        return testClassNameToStubClassPath(testClassName);
    }

    public static String getStubClassPathOfTestMethod(ExtensionContext extensionContext) {
        final Optional<Method> optionalMethod = extensionContext.getTestMethod();
        if (optionalMethod.isEmpty())
            throw new IllegalArgumentException("Not test method available in the extension context");

        return getStubClassPath(optionalMethod.get());
    }

    public static String getStubFilePathOfTestClass(ExtensionContext extensionContext) {
        return classPathToFilePath(getStubClassPathOfTestClass(extensionContext));
    }

    public static String getStubFilePathOfTestMethod(ExtensionContext extensionContext) {
        return classPathToFilePath(getStubClassPathOfTestMethod(extensionContext));
    }
}