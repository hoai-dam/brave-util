package brave.extension;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Home of all stubbing facilities.
 */
public class BraveTestContext {

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private static DatabaseStub databaseStub;

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private static KafkaStub kafkaStub;

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private static WireMockStub wireMockStub;

}
