package brave.matcher;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.util.HashMap;

public class ObjectMatcherAssert<T> {
    private static final String lineSeparator = System.lineSeparator();

    private final T actual;
    private final CombinableMatcher<HashMap<String, Object>> combinableMatcher = new CombinableMatcher<>();
    private final String description;

    private ObjectMatcherAssert(T actual, String description) {
        this.actual = actual;
        this.description = description;
    }

    public static <T> ObjectMatcherAssert<T> assertThat(T actual, String description) {
        return new ObjectMatcherAssert<>(actual, description);
    }

    public ObjectMatcherAssert<T> has(String property, Matcher<? super Object> objectMatcher) {
        this.combinableMatcher.and(HashMapMatcher.has(property, objectMatcher));
        return this;
    }

    public void verify() {
        if (!combinableMatcher.matches(actual)) {
            Description mismatchDescription = new StringDescription();
            mismatchDescription.appendText(lineSeparator + description)
                    .appendText(lineSeparator)
                    .appendText("But: ");
            combinableMatcher.describeMismatch(actual, mismatchDescription);

            throw new AssertionError(mismatchDescription.toString());
        }
    }
}
