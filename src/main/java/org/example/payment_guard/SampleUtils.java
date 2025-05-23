package org.example.payment_guard;

import com.example.test1.entity.MenuItem;
import com.example.test1.entity.ReceiptData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Utility methods for transforming ReceiptData and calling Gemini API.
 */
public class SampleUtils {

    /**
     * Converts a list of MenuItem to its JSON representation using Avro JSON Encoder.
     */
    public static String menuItemsToJson(List<MenuItem> items) {
        try {
            StringBuilder jsonArray = new StringBuilder("[");
            DatumWriter<MenuItem> writer = new SpecificDatumWriter<>(MenuItem.class);

            for (int i = 0; i < items.size(); i++) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Encoder encoder = EncoderFactory.get().jsonEncoder(MenuItem.getClassSchema(), out);
                writer.write(items.get(i), encoder);
                encoder.flush();
                out.close();

                if (i > 0) jsonArray.append(",");
                jsonArray.append(out.toString());
            }

            jsonArray.append("]");
            return jsonArray.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize menu items to JSON", e);
        }
    }

    /**
     * Converts an Iterable to a List.
     */
    public static <T> List<T> toList(Iterable<T> iterable) {
        List<T> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }

    /**
     * Dummy asynchronous call to Gemini API for the given batch.
     * Replace this with a real HTTP client implementation.
     */
    public static CompletableFuture<ReportResult> callGeminiApi(List<ReceiptData> batch) {
        return CompletableFuture.supplyAsync(() -> {
            ReportResult result = new ReportResult();
            if (!batch.isEmpty()) {
                result.setFranchiseId(batch.get(0).getFranchiseId());
                result.setStoreId(batch.get(0).getStoreId());
                result.setAnalysisTime(LocalDateTime.now());
                result.setTotalSales(batch.stream().mapToInt(ReceiptData::getTotalPrice).sum());
                result.setTopMenu("dummy_item");
                result.setAnomalyScore(0.0);
                result.setSummaryText("This is a dummy summary.");
                result.setFullResultJson("{}");
            }
            return result;
        });
    }
}
