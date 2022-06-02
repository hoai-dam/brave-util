package brave.matcher;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MatcherTest {

    @Test
    public void testMatcher_shouldBeReturnError() {
        HashMap<String, Object> mapActualUser = new HashMap<>();
        mapActualUser.put("tiki_id", 1L);
        mapActualUser.put("phone", "test");
        mapActualUser.put("email", "test");

        // Then
        User expected = new User();
        expected.setTikiId(2L)
                .setPhone("test2")
                .setEmail("test2");

        ObjectMatcherAssert.assertThat(mapActualUser,"User from database should be the same as expected")
                .has("tiki_id", equalTo(expected.getTikiId()))
                .has("email", equalTo(expected.getEmail()))
                .has("phone", equalTo(expected.getPhone()))
                .verify();
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

        ObjectMatcherAssert.assertThat(mapActualUser,"User from database should be the same as expected")
                .has("tiki_id", equalTo(expected.getTikiId()))
                .has("email", equalTo(expected.getEmail()))
                .has("phone", equalTo(expected.getPhone()))
                .verify();
    }
}
