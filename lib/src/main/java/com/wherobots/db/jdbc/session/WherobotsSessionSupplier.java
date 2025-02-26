package com.wherobots.db.jdbc.session;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.wherobots.db.AppStatus;
import com.wherobots.db.Region;
import com.wherobots.db.Runtime;
import com.wherobots.db.SessionType;
import com.wherobots.db.jdbc.serde.JsonUtil;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.core.functions.CheckedSupplier;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.hc.core5.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * Requests and waits for a Wherobots SQL Session.
 *
 * @author mpetazzoni
 */
public abstract class WherobotsSessionSupplier {

    private static final Logger logger = LoggerFactory.getLogger(WherobotsSessionSupplier.class);

    private static final String SQL_SESSION_ENDPOINT = "https://%s/sql/session?region=%s";
    private static final String PROTOCOL_VERSION = "1.0.0";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record SqlSessionRequestPayload(
        String runtimeId,
        Integer shutdownAfterInactiveSeconds,
        String sessionType) {}
    private record SqlSessionAppMeta(String url) {}
    private record SqlSessionResponsePayload(AppStatus status, SqlSessionAppMeta appMeta) {}

    /**
     * Requests the creation of a SQL Session from the Wherobots Cloud API, waits for it to be ready, and connects to it.
     *
     * @param host
     * @param runtime
     * @param region
     * @param headers
     * @return
     * @throws SQLException
     */
    public static WherobotsSession create(String host, Runtime runtime, Region region, SessionType sessionType, Map<String, String> headers)
        throws SQLException {
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        RetryConfig config = RetryConfig.custom()
                .maxAttempts(100)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(1000, 1.5, 10000))
                .retryExceptions(IOException.class)
                .retryOnResult(Objects::isNull)
                .build();
        Retry retry = RetryRegistry.of(config).retry("session");

        try {
            URI sessionIdUri = new SqlSessionSupplier(client, headers, host, runtime, region, sessionType).get();
            URI wsUri = Retry.decorateCheckedSupplier(retry, new SessionWsUriSupplier(client, headers, sessionIdUri)).get();
            return create(wsUri, headers);
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException("Failed to connect to SQL session!", t);
        }
    }

    /**
     * Connects to an existing SQL Session directly from its WebSocket URI.
     *
     * @param wsUri
     * @param headers
     * @return
     * @throws SQLException
     */
    public static WherobotsSession create(URI wsUri, Map<String, String> headers)
            throws SQLException {
        logger.info("Connecting to SQL Session at {} ...", wsUri);
        try {
            return new WherobotsSession(wsUri, headers);
        } catch (Exception e) {
            throw new SQLException("Failed to connect to SQL session!", e);
        }
    }

    /**
     * Requests a new SQL session from Wherobots and returns the session ID URI we get redirected to.
     */
    private record SqlSessionSupplier(HttpClient client,
                                      Map<String, String> headers,
                                      String host,
                                      Runtime runtime,
                                      Region region,
                                      SessionType sessionType)
            implements CheckedSupplier<URI> {

        @Override
        public URI get() throws IOException, InterruptedException {
            logger.info("Requesting {} runtime using {} session type in {} from {}...",
                    runtime.name, sessionType.name, region.name, host);

            HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString(
                    JsonUtil.serialize(new SqlSessionRequestPayload(
                        runtime.name,
                        null,
                        sessionType.name)));

            HttpRequest.Builder request = HttpRequest.newBuilder()
                    .POST(body)
                    .uri(URI.create(String.format(SQL_SESSION_ENDPOINT, host, region.name)))
                    .header("Content-Type", "application/json");
            headers.forEach(request::header);

            HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HttpStatus.SC_OK) {
                throw new IllegalStateException(String.format("Got %d from SQL session at %s.", response.statusCode(), host));
            }
            return response.uri();
        }
    }

    /**
     * Queries the given session ID URI and returns the SQL Session WebSocket URI if the session is ready.
     */
    private record SessionWsUriSupplier(HttpClient client,
                                        Map<String, String> headers,
                                        URI sessionIdUri)
            implements CheckedSupplier<URI> {

        private static final Map<String, String> HTTP_TO_WS_MAP = Map.of(
                "http", "ws",
                "https", "wss");

        private URI httpToWsUri(URI httpUri) throws URISyntaxException {
            return new URI(
                    HTTP_TO_WS_MAP.getOrDefault(httpUri.getScheme(), httpUri.getScheme()),
                    httpUri.getUserInfo(),
                    httpUri.getHost(),
                    httpUri.getPort(),
                    httpUri.getPath(),
                    httpUri.getQuery(),
                    httpUri.getFragment()
            );
        }

        @Override
        public URI get() throws IOException, InterruptedException, URISyntaxException {
            logger.info("Waiting for session {} ...", sessionIdUri);

            HttpRequest.Builder request = HttpRequest.newBuilder()
                    .GET()
                    .uri(sessionIdUri);
            headers.forEach(request::header);

            HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
            SqlSessionResponsePayload payload = JsonUtil.deserialize(response.body(), new TypeReference<>() {});
            logger.info("  ... {}", payload.status);

            if (payload.status.isStarting()) {
                // Return null to indicate to the retry logic we're still waiting for a result.
                return null;
            } else if (payload.status == AppStatus.READY) {
                return httpToWsUri(new URI(String.format("%s/%s", payload.appMeta.url, PROTOCOL_VERSION)));
            } else {
                throw new IllegalStateException(
                        String.format("Failed to create SQL session: %s", payload.status));
            }
        }
    }
}
