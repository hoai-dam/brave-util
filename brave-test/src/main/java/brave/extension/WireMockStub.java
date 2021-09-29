

package brave.extension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static brave.extension.util.Config.PROJECT_RESOURCE_PATH;

/**
 * This class allow you to load/unload stub mappings at will. Stub mappings should
 * be provided in a standard wiremock folder with:
 * <br>
 * <ul>
 *     <li>{@code mappings} folder containing multiple mappings</li>
 *     <li>{@code __files} folder containing serving files</li>
 * </ul>
 * When you load/unload the stub mappings, please provide the classpath to the folder.
 * For example if the stubbing is at {@code /src/test/resources/path/to/wiremock/folder},
 * then you should use {@code path/to/wiremock/folder} as the parameter.
 * <br><br>
 * You can obtain an instance of this class at every test cases:
 * <pre>{@code
 * @ExtendWith({brave.extension.WireMockExtension.class})
 * class ImportantTest {
 *     @org.junit.jupiter.api.Test
 *     void something_shouldBe_okTest(brave.extension.WiremockStub wiremockStub) {
 *
 *     }
 * }
 * }</pre>
 */
@Slf4j
public class WireMockStub {

    private static final String JSON_SUFFIX = ".json";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Getter private final WireMockServer wireMockServer;
    @Getter private final String wireMockBaseUrl;

    public WireMockStub(WireMockServer wireMockServer, String wiremockBaseUrl) {
        this.wireMockServer = wireMockServer;
        this.wireMockBaseUrl = wiremockBaseUrl;
    }

    public void load(String stubClassPath) throws IOException {
        Path wireMockStubsPath = Paths.get(PROJECT_RESOURCE_PATH, stubClassPath);
        loadStubMappings(wireMockStubsPath);
        loadStubFiles(wireMockStubsPath);
    }

    public void unload(String stubClassPath) throws IOException {
        Path wireMockStubsPath = Paths.get(PROJECT_RESOURCE_PATH, stubClassPath);
        resetStubMappings();
        removeStubFiles(wireMockStubsPath);
    }

    private void loadStubMappings(Path wireMockStubs) throws IOException {
        Path mappingsFolderPath = wireMockStubs.resolve("mappings");
        log.warn("Loading 'mappings' at " + mappingsFolderPath);
        File[] mappingFiles = mappingsFolderPath.toFile().listFiles();

        if (mappingFiles == null) {
            log.warn("No wiremock mapping exists in {}", mappingsFolderPath);
            return;
        }

        for (final File mappingFile : mappingFiles) {
            if (mappingFile.isDirectory()) continue;
            if (!mappingFile.getName().endsWith(JSON_SUFFIX)) continue;

            JsonNode stub = objectMapper.readTree(mappingFile);

            if (stub.has("mappings")) {
                for (JsonNode sm : stub.get("mappings")) {
                    createStubMapping(sm);
                }
            } else {
                createStubMapping(stub);
            }
        }
    }

    private void resetStubMappings() throws IOException {
        log.warn("Resetting stub mappings");
        Request.Post(wireMockBaseUrl + "/__admin/reset")
                .execute()
                .handleResponse(this::handleHttpResponse);
    }

    @SneakyThrows
    private void loadStubFiles(Path wireMockStubs) {
        Path stubFilesFolderPath = wireMockStubs.resolve("__files");
        log.warn("Loading '__files' at " + stubFilesFolderPath);
        File[] stubFiles = stubFilesFolderPath.toFile().listFiles();

        if (stubFiles == null) {
            log.warn("No wiremock files exists in {}", stubFilesFolderPath);
            return;
        }

        for (final File stubFile : stubFiles) {
            if (stubFile.isDirectory()) continue;
            uploadStubFile(stubFile);
        }
    }

    private void removeStubFiles(Path wiremockStubs) {
        Path stubFilesFolderPath = wiremockStubs.resolve("__files");
        log.warn("Resetting '__files' at " + stubFilesFolderPath);
        File[] stubFiles = stubFilesFolderPath.toFile().listFiles();
        if (stubFiles == null) return;
        for (final File stubFile : stubFiles) {
            if (stubFile.isDirectory()) continue;
            removeStubFile(stubFile);
        }
    }

    @SneakyThrows
    private void createStubMapping(JsonNode mapping) {
        Request.Post(wireMockBaseUrl + "/__admin/mappings")
                .body(new StringEntity(
                        mapping.toString(),
                        ContentType.APPLICATION_JSON))
                .execute()
                .handleResponse(this::handleHttpResponse);
    }

    @SneakyThrows
    private void uploadStubFile(File stubFile) {
        byte[] stubFileContent = FileUtils.readFileToByteArray(stubFile);

        Request.Put(wireMockBaseUrl + "/__admin/files/" + stubFile.getName())
                .body(new ByteArrayEntity(stubFileContent, ContentType.APPLICATION_JSON))
                .execute()
                .handleResponse(this::handleHttpResponse);
    }

    @SneakyThrows
    private void removeStubFile(File stubFile) {
        Request.Delete(wireMockBaseUrl + "/__admin/files/" + stubFile.getName())
                .execute()
                .handleResponse(this::handleHttpResponse);
    }

    private String handleHttpResponse(org.apache.http.HttpResponse httpResponse) throws IOException {
        try (InputStream bodyStream = httpResponse.getEntity().getContent()) {
            String body = IOUtils.toString(bodyStream, StandardCharsets.UTF_8);
            StatusLine statusLine = httpResponse.getStatusLine();
            String fullResponse = String.format("\n%s\n%s\n", statusLine, body);
            if (statusLine.getStatusCode() >= 300) {
                log.error(fullResponse);
            }
            return fullResponse;
        }
    }
}
