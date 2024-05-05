package com.wherobots.db.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Requests and waits for a Wherobots SQL Session.
 *
 * @author mpetazzoni
 */
public class WherobotsSessionSupplier implements Supplier<WherobotsSession> {

    public static final String SQL_SESSION_ENDPOINT = "https://%s/sql/session?region=%s";

    private record SqlSessionRequestPayload(String runtimeId) {}

    public WherobotsSessionSupplier(String host, Runtime runtime, Region region, Map<String, String> headers)
            throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        HttpRequest.Builder request = HttpRequest.newBuilder()
            .uri(URI.create(String.format(SQL_SESSION_ENDPOINT, host, region.name)))
            .header("Content-Type", "application/json");
        headers.forEach(request::header);

        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString(
                JsonUtil.serialize(new SqlSessionRequestPayload(runtime.name)));
        request.POST(body);

        HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        URI sessionIdUri = response.uri();

    }

    @Override
    public WherobotsSession get() {
        return null;
    }
}
