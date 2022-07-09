package brave.matcher;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static brave.matcher.ObjectMatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class MatcherTest {

    @Test
    public void testMatcher_shouldBeReturnError() {
        AssertionError error = Assertions.assertThrows(AssertionError.class, () -> {
            HashMap<String, Object> actualUser = new HashMap<>();
            actualUser.put("tiki_id", 1L);
            actualUser.put("phone", "test");
            actualUser.put("email", "test");

            // Then
            User expectedUser = new User();
            expectedUser.setTikiId(2L)
                    .setPhone("test2")
                    .setEmail("test2");

            assertThat(actualUser, "User from database should be the same as expected")
                    .has("tiki_id", equalTo(expectedUser.getTikiId()))
                    .has("email", equalTo(expectedUser.getEmail()))
                    .has("phone", equalTo(expectedUser.getPhone()))
                    .verify();
        });

        MatcherAssert.assertThat(error.getMessage(), containsString("User from database should be the same as expected"));
        MatcherAssert.assertThat(error.getMessage(), containsString("\"tiki_id\" expected <2L> but actual was <1L>"));
        MatcherAssert.assertThat(error.getMessage(), containsString("\"email\" expected \"test2\" but actual was \"test\""));
        MatcherAssert.assertThat(error.getMessage(), containsString("\"phone\" expected \"test2\" but actual was \"test\""));
    }

    @Test
    public void testMatcher_shouldBeReturnOk() {
        HashMap<String, Object> mapActualUser = new HashMap<>();
        mapActualUser.put("tiki_id", 1L);
        mapActualUser.put("phone", "test");
        mapActualUser.put("email", "test");

        // Then
        User expected = new User();
        expected.setTikiId(1L)
                .setPhone("test")
                .setEmail("test");

        assertThat(mapActualUser, "User from database should be the same as expected")
                .has("tiki_id", equalTo(expected.getTikiId()))
                .has("email", equalTo(expected.getEmail()))
                .has("phone", equalTo(expected.getPhone()))
                .verify();
    }
}
