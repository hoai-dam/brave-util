package brave.extension.util;

import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class ResourcesPathUtil {

    public static String filePathToClassPath(String filePath) {
        return filePath.substring(Config.PROJECT_RESOURCE_PATH.length());
    }

    public static Path classPathToFilePath(String classPath) {
        return Paths.get(Config.PROJECT_RESOURCE_PATH, classPath);
    }

    /**
     * stubs/ClassName/methodName
     */
    public static String getStubClassPath(Method testMethod) {
        String methodName = testMethod.getName();
        String className = testMethod.getDeclaringClass().getSimpleName();
        String classStubFolder = className.endsWith(Config.TEST_SUFFIX) ? trimEnd(className, Config.TEST_SUFFIX) : className;

        return "stubs/" + classStubFolder + "/" + methodName;
    }

    public static String trimEnd(String original, String ending) {
        return original.substring(0, original.length() - ending.length());
    }

    public static String getStubClassPathOfTestMethod(ExtensionContext extensionContext) {
        final Optional<Method> optionalMethod = extensionContext.getTestMethod();
        if (optionalMethod.isEmpty())
            throw new IllegalArgumentException("Not test method available in the extension context");

        return getStubClassPath(optionalMethod.get());
    }

    public static Path getStubFilePathOfTestMethod(ExtensionContext extensionContext) {
        return classPathToFilePath(getStubClassPathOfTestMethod(extensionContext));
    }
}