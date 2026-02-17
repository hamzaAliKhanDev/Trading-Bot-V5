package com.deltaexchange.trade.service;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class TestPositionClient {

    // TODO: PUT YOUR KEYS HERE
     private static final String API_KEY = "QkcozvN7XM6uzcRC5lREH6rdDXQI4i";
     private static final String API_SECRET = "IeWvF36I8wABBseGQwKZ9OoR3THYbDt78jfeCU8aj78iuXSCBbeOYHGtDaC5";

    //private static final String API_KEY = "GIW1raNFd4iY9CDWemOt2HdZFIAuNj";
    //private static final String API_SECRET = "07PkTvdkUC4EuMrLAwn4qib77XlReOoUmj879tmKO2ar0BMNzv6qdtZOM4mp";

    private static final String BASE_URL = "https://api.india.delta.exchange";

    public static void main(String[] args) {
        try {
            TestPositionClient client = new TestPositionClient();
            client.getPosition(27);
        } catch (Exception e) {
            System.out.println("FATAL ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void getPosition(int productId) {
        try {
            String endpoint = "/v2/positions";
            String query = "product_id=" + productId;

            long timestamp = Instant.now().getEpochSecond();

            // --- SIGNATURE BUILDING ---
            String prehash = "GET" + timestamp + endpoint + "?" + query;

            System.out.println("=== DEBUG SIGNING INFO ===");
            System.out.println("Timestamp    : " + timestamp);
            System.out.println("Prehash      : " + prehash);

            String signature = hmacSHA256(prehash, API_SECRET);
            System.out.println("Signature(hex): " + signature);
            System.out.println("=== END DEBUG ===");

            String url = BASE_URL + endpoint + "?" + query;

            HttpClient http = HttpClient.newHttpClient();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("api-key", API_KEY)
                    .header("signature", signature)
                    .header("timestamp", String.valueOf(timestamp))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            System.out.println("Sending request to: " + url);

            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

            System.out.println("HTTP Status : " + response.statusCode());
            System.out.println("Response Body:");
            System.out.println(response.body());

            if (response.statusCode() == 401) {
                System.out.println("ERROR 401: Unauthorized");
                System.out.println("Possible reasons:");
                System.out.println("1) Wrong signature");
                System.out.println("2) Wrong timestamp (should match server time)");
                System.out.println("3) Wrong API key or secret");
                System.out.println("4) Your IP is NOT whitelisted in Delta Exchange India");
            }

        } catch (Exception e) {
            System.out.println("Exception calling getPosition(): " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String hmacSHA256(String data, String secret) {
        try {
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            sha256_HMAC.init(secretKey);

            byte[] hashBytes = sha256_HMAC.doFinal(data.getBytes(StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();

        } catch (Exception e) {
            System.out.println("Error generating HMAC: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
