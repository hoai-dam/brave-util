package brave.matcher;

import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.regex.Pattern;

import static brave.matcher.Verifier.verify;

@Slf4j
public class HashMapMatcher {
    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("^\\{\\w*=\\w*}$");

    public static Matcher<HashMap<String, Object>> has(String property, Matcher<? super Object> matcher) {
        return new TypeSafeMatcher<HashMap<String, Object>>() {
            final Description mismatchDescription = new StringDescription();

            @Override
            protected boolean matchesSafely(HashMap<String, Object> actual) {
                Object value = featureValueOf(actual);
                return verify(value, matcher, mismatchDescription);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(String.format("\"%s\" expected ", property)).appendDescriptionOf(matcher);
            }

            @SuppressWarnings("unchecked")
            Object featureValueOf(HashMap<String, Object> actual) {
                final String dotSeparator = "\\.";
                String[] subProperties = property.split(dotSeparator);
                Object data = new HashMap<>(actual);
                for (String s : subProperties) {
                    if (data instanceof HashMap) {
                        data = ((HashMap<String, Object>) data).get(s);
                    } else if (data instanceof ArrayList) {
                        if (KEY_VALUE_PATTERN.matcher(s).matches()) {
                            String[] keyValuePairArr = s.substring(1, s.length() - 1).split("=");
                            String subKey = keyValuePairArr[0];
                            String subValue = keyValuePairArr[1];
                            data = ((ArrayList<HashMap<String, Object>>) Objects.requireNonNull(data)).stream()
                                    .filter(obj -> obj.get(subKey).toString().equals(subValue))
                                    .findFirst().orElse(null);
                        } else {
                            data = ((ArrayList<Object>) data).get(Integer.parseInt(s));
                        }
                    }
                }
                return data;
            }

            @Override
            protected void describeMismatchSafely(HashMap<String, Object> theMismatchItem, Description description) {
                description.appendText(mismatchDescription.toString());
            }
        };
    }
}
