package brave.matcher;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.LinkedList;
import java.util.List;

public class CombinableMatcher<T> extends BaseMatcher<T> implements Matcher<T> {
    private final List<Matcher<? super T>> matchers = new LinkedList<>();
    private final List<Matcher<? super T>> mismatches = new LinkedList<>();

    private CombinableMatcher(final Matcher<? super T> matcher) {
        matchers.add(matcher);
    }

    protected CombinableMatcher() { }

    public static <T> CombinableMatcher<T> allOf(final Matcher<? super T> matcher) {
        return new CombinableMatcher<>(matcher);
    }

    public CombinableMatcher<T> and(final Matcher<? super T> matcher) {
        matchers.add(matcher);
        return this;
    }

    @Override
    public boolean matches(final Object o) {
        boolean result = true;
        for (final Matcher<? super T> matcher : matchers) {
            if (!matcher.matches(o)) {
                mismatches.add(matcher);
                result = false;
            }
        }
        return result;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendList("(", " " + "and" + " ", ")", matchers);
    }

    @Override
    public void describeMismatch(final Object item, final Description description) {
        int mismatchCount = 1;
        final String lineSeparator = System.lineSeparator();
        description.appendText(lineSeparator);
        for (final Matcher<? super T> matcher : mismatches) {
            description.appendText(String.format("%d. ", mismatchCount));
            description.appendDescriptionOf(matcher).appendText(" but actual ");
            matcher.describeMismatch(item, description);
            description.appendText(lineSeparator);
            mismatchCount++;
        }
    }
}
