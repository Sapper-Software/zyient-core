package io.zyient.core.mapping.mapper;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.internal.path.PathCompiler;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.core.mapping.mapper.db2.DBMapper;
import io.zyient.core.mapping.mapper.db2.DBMappingConf;
import io.zyient.core.mapping.mapper.db2.MappedElementWithConf;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.MappingType;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JPathMapTransformer<T> implements IMapTransformer<T> {
    private final Class<? extends T> type;
    private final MappingSettings settings;
    private final Map<String, Map<Integer, MapNode>> mapper = new HashMap<>();
    private final Configuration configuration;
    private final Configuration configurationPath;
    private final DBMapper dbMapper;


    public JPathMapTransformer(@NonNull Class<? extends T> type, @NonNull MappingSettings settings, DBMapper dbMapper) {
        this.type = type;
        this.settings = settings;
        this.configuration = Configuration.defaultConfiguration()
                .jsonProvider(new GsonJsonProvider())
                .mappingProvider(new GsonMappingProvider());
        this.configurationPath = Configuration.defaultConfiguration()
                .jsonProvider(new GsonJsonProvider())
                .mappingProvider(new GsonMappingProvider()).setOptions(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS);
        this.dbMapper = dbMapper;
    }

    @Override
    public IMapTransformer<T> add(@NonNull MappedElement element) throws Exception {
        Preconditions.checkArgument(element instanceof MappedElementWithConf);
        MappedElementWithConf mec = (MappedElementWithConf) element;
        List<DBMapper.DBConfNode> nodes = this.dbMapper.getNodes();
        for (DBMapper.DBConfNode node : nodes) {
            if (node.getConf().getKey().getKey().equals(mec.getConf().entityKey().getKey())) {
                MapNode mapNode = convertToMapNode(node);
                mapNode.nodes = buildChildNode(node);
                Map<Integer, MapNode> nodeMap = new HashMap<>();
                nodeMap.put(0, mapNode);
                mapper.put(mec.getSourcePath(), nodeMap);
            }
        }
        return this;
    }

    private Map<String, MapNode> buildChildNode(DBMapper.DBConfNode node) {
        Map<String, MapNode> childNode = new HashMap<>();
        if (CollectionUtils.isEmpty(node.getNodes())) {
            return null;
        }
        node.getNodes().forEach(c -> {
            MapNode conf = convertToMapNode(c);
            conf.nodes = buildChildNode(c);
            childNode.put(c.getConf().getSourcePath(), conf);
        });
        return childNode;
    }

    private MapNode convertToMapNode(DBMapper.DBConfNode confNode) {
        MapNode node = new MapNode();
        node.mappingType = MappingType.Field;
        node.targetPath = confNode.getConf().getTargetPath();
        node.type = type;
        node.name = confNode.getConf().getSourcePath();
        node.nullable = true;
        node.reference = confNode;
        return node;
    }

    @Override
    public Object transform(@NonNull MappedElement element, @NonNull Object source) throws Exception {
        return source;
    }

    @Override
    public Map<String, Object> transform(@NonNull Map<String, Object> source, @NonNull Class<? extends T> entityType, @NonNull Context context) throws Exception {
        Object o = JSONUtils.createBlankJson(entityType);

        Map<String, Object> data = new Gson().fromJson(new Gson().toJson(o), Map.class);
        JSONUtils.checkAndAddType(data, entityType);

        for (String key : mapper.keySet()) {
            Map<Integer, MapNode> nodes = mapper.get(key);
            for (Integer seq : nodes.keySet()) {
                transform(source, nodes.get(seq), data, context.params);
            }
        }
        return data;
    }

    private void transform(@NonNull Map<String, Object> source, @NonNull MapNode node, @NonNull Map<String, Object> data, @NonNull Map<String, Object> params) throws Exception {

        DocumentContext documentContext = JsonPath.parse(new Gson().toJson(source), configuration);
        DocumentContext targetContext = JsonPath.parse(new Gson().toJson(data), configuration);
        if (!CollectionUtils.isEmpty(node.nodes)) {
            JsonElement element = documentContext.read(node.name);
            JsonElement targetEl = targetContext.read(node.targetPath);
            JsonElement targetChildEl;
            if (targetEl.isJsonArray()) {
                targetChildEl = targetEl.getAsJsonArray().get(0);
            } else {
                targetChildEl = targetEl.getAsJsonObject();
            }
            List<Map> childRows =  new ArrayList<>();
            if (element.isJsonArray()) {
                element.getAsJsonArray().forEach(c -> {
                    DocumentContext childDocContext = JsonPath.parse(c.toString(), configuration);
                    DocumentContext childTargetContext = JsonPath.parse(targetChildEl.toString(), configuration);
                    Map childData =  new Gson().fromJson(targetChildEl.toString(),Map.class);
                    node.nodes.forEach((n, v) -> {
                        transformNode(v, childDocContext, childTargetContext, childData);
                    });
                    childRows.add(childData);
                });
                setValue(node.targetPath,childRows,targetContext);
                Map map = new Gson().fromJson(targetContext.jsonString(), Map.class);
                data.putAll(map);
            }

        } else {
            transformNode(node, documentContext, targetContext, data);
        }

    }

    private void transformNode(MapNode node, DocumentContext documentContext, DocumentContext targetContext, Map<String, Object> data) {
        JsonElement jsonElement = documentContext.read(node.name);
        if (jsonElement.isJsonArray()) {
            JsonArray jsonArray = (JsonArray) jsonElement;
            iterateArrayAndTransform(jsonArray, node, targetContext);
        }
        Map map = new Gson().fromJson(targetContext.jsonString(), Map.class);
        data.putAll(map);
    }

    private void iterateArrayAndTransform(JsonArray jsonArray, MapNode node, DocumentContext targetContext) {
        jsonArray.forEach(je -> {
            transformElement(je, node, targetContext);
        });
    }

    private void transformElement(JsonElement jsonElement, MapNode mapNode, DocumentContext targetContext) {
        if (jsonElement.isJsonPrimitive()) {
            transformPrimitive(jsonElement, mapNode, targetContext);
        } else if (jsonElement.isJsonArray()) {
            iterateArrayAndTransform(jsonElement.getAsJsonArray(), mapNode, targetContext);
        } else if (jsonElement.isJsonObject()) {
            Set<Map.Entry<String, JsonElement>> entrySet = jsonElement.getAsJsonObject().entrySet();
            entrySet.forEach(c -> {
                transformElement(c.getValue(), mapNode, targetContext);
            });
        }
    }

    private void transformPrimitive(JsonElement jsonElement, MapNode mapNode, DocumentContext targetContext) {
        String next = jsonElement.getAsString();
        setValue(mapNode.targetPath, next, targetContext);
    }

    private void setValue(String targetPath, Object v, DocumentContext targetContext) {
        DocumentContext pc = JsonPath.parse(targetContext.jsonString(), configurationPath);
        JsonElement jsonElement = pc.read(targetPath);
        if (jsonElement.isJsonArray() && !jsonElement.getAsJsonArray().isEmpty()) {
            targetPath = jsonElement.getAsJsonArray().get(0).getAsString();
        } else {
            targetPath = jsonElement.getAsJsonObject().getAsString();
        }
        targetPath = targetPath.replace("$", "");
        List<String> paths = new ArrayList<>();

        String rePattern = "\\[(.*?)]";
        Pattern p = Pattern.compile(rePattern);
        Matcher m = p.matcher(targetPath);
        while (m.find()) {
            paths.add(m.group(1));
        }
        List<String> subPaths = new ArrayList<>();
        for (String path : paths) {
            String abPath = String.format("[%s]", path);
            subPaths.add(String.join("", subPaths.toArray(new String[0])) + abPath);
        }
        String leafPath = "";
        for (int i = 0; i < subPaths.size(); i++) {
            String c = subPaths.get(i);
            String path = "$" + c;
            leafPath = path;
            JsonElement jsonElement1 = targetContext.read(path);
            if (jsonElement1.isJsonNull()) {
                // node not exists .. create it.
                if (StringUtils.isNumeric(c)) {
                    targetContext.put(path, c, new ArrayList<>());
                } else if (i == subPaths.size() - 1) {
                    targetContext.put(path, c, v);
                } else {
                    targetContext.put(path, c, new HashMap<>());
                }
            }
        }
        targetContext.set(leafPath, v);
    }

}
