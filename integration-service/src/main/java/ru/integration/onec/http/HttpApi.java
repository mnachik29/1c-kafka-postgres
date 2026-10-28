package ru.integration.onec.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/** HTTP-сервер сервиса: маршруты регистрируют отдельные endpoint-классы. */
public class HttpApi {

    private final HttpServer server;

    public HttpApi(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        // POST /sync выполняется в потоке запроса — /health не должен ждать её окончания
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    public HttpApi route(String path, HttpHandler handler) {
        server.createContext(path, handler);
        return this;
    }

    public void start() {
        server.start();
    }

    static void send(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
