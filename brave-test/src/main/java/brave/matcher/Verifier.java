package brave.matcher;

import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class Verifier {
    public static <T> boolean verify(T a, Matcher<? super T> matcher, Description mismatchDescription) {
        boolean result = verify(a, matcher);
        if (!result) matcher.describeMismatch(a, mismatchDescription);

        return result;
    }

    public static <T> boolean verify(T a, Matcher<? super T> matcher) {
        return matcher.matches(a);
    }
}

