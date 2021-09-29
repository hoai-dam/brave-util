package brave.extension;

import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

@ExtendWith({WireMockExtension.class})
public class WireMockExtensionTest {

    @Test
    void wiremockLoading_shouldBeOk(WireMockStub wiremockStub) throws IOException, InterruptedException {
        // With context
        wiremockStub.load("stubs/wiremockLoading");

        given()
                .accept(ContentType.JSON)
                .baseUri(wiremockStub.getWireMockBaseUrl())
                .get("/greeting")
                .then()
                .statusCode(200)
                .body(containsString("Hello world!!!"))
        ;

        // Clean up
        wiremockStub.unload("stubs/wiremockLoading");
    }
}
