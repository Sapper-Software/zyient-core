package io.zyient.core.mapping;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.junit.jupiter.api.Test;

public class JsonPathTest {
    String json = "{\n" +
            "  \"store\":\n" +
            "  {\n" +
            "  \"book\":\n" +
            "  [\n" +
            "  {\n" +
            "  \"category\": \"reference\",\n" +
            "  \"author\": \"Nigel Rees\",\n" +
            "  \"title\": \"Sayings of the Century\",\n" +
            "  \"price\": 8.95\n" +
            "  },\n" +
            "  {\n" +
            "  \"category\": \"fiction\",\n" +
            "  \"author\": \"Evelyn Waugh\",\n" +
            "  \"title\": \"Sword of Honour\",\n" +
            "  \"price\": 12.99\n" +
            "  }\n" +
            "  ],\n" +
            "  \"bicycle\":\n" +
            "  {\n" +
            "  \"color\": \"red\",\n" +
            "  \"price\": 19.95\n" +
            "  }\n" +
            "  }\n" +
            "  }";

    @Test
    public void test() {
        Configuration configuration = Configuration.defaultConfiguration()
                .jsonProvider(new GsonJsonProvider())
                .mappingProvider(new GsonMappingProvider()).addOptions(Option.AS_PATH_LIST);
        DocumentContext documentContext = JsonPath.parse(json, configuration);
        JsonElement jsonElement = documentContext.read("$.store.book[*]");

        // System.out.println(jsonElement.isJsonArray());
        if (jsonElement.isJsonArray()) {
            JsonArray jsonArray = (JsonArray) jsonElement;
            jsonArray.forEach(je -> {
                String childJson = je.toString();
                DocumentContext childContext = JsonPath.parse(childJson, configuration);
                JsonElement childElement = childContext.read("$.category");
                if (childElement.isJsonPrimitive()) {
                    System.out.println(childElement.getAsString());
                }
            });
        }
    }

}
