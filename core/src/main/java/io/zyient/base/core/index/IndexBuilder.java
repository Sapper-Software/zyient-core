/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.core.index;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.NonNull;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class IndexBuilder {
    private final Map<String, Indexer<?>> indexers = new HashMap<>();

    public Document build(@NonNull Object source) throws Exception {
        synchronized (indexers) {
            Document doc = new Document();
            Field[] fields = ReflectionHelper.getAllFields(source.getClass());
            if (fields == null) {
                throw new Exception(String.format("No fields found. [type=%s]", source.getClass().getCanonicalName()));
            }
            for (Field field : fields) {
                Object value = ReflectionHelper.reflectionUtils().getFieldValue(source, field);
                if (value != null)
                    build(null, doc, field, value);
            }
            return doc;
        }
    }

    private void build(String path, Document doc, Field field, Object value) throws Exception {
        if (!field.isAnnotationPresent(Indexed.class)) {
            return;
        }
        Indexed indexed = field.getAnnotation(Indexed.class);
        if (Strings.isNullOrEmpty(path)) {
            path = field.getName();
        } else {
            path = String.format("%s.%s", path, field.getName());
        }
        String key = path;
        if (!Strings.isNullOrEmpty(indexed.name())) {
            key = indexed.name();
        }
        Indexer<?> indexer = getIndexer(indexed.indexer());
        if (indexer != null) {
            String v = indexer.index(value);
            if (!Strings.isNullOrEmpty(v)) {
                doc.add(new TextField(key, v, indexed.stored()));
            }
        } else {
            if (ReflectionHelper.isPrimitiveTypeOrString(field)) {
                if (ReflectionHelper.isNumericType(field.getType())) {
                    if (ReflectionHelper.isLong(field.getType())
                            || ReflectionHelper.isInt(field.getType())
                            || ReflectionHelper.isShort(field.getType())) {
                        doc.add(new LongField(key, (long) value, indexed.stored()));
                    } else if (ReflectionHelper.isFloat(field.getType())
                            || ReflectionHelper.isDouble(field.getType())) {
                        doc.add(new DoubleField(key, (double) value, indexed.stored()));
                    } else if (ReflectionHelper.isBoolean(field.getType())) {
                        Boolean b = (Boolean) value;
                        doc.add(new TextField(key, (b ? "true" : "false"), indexed.stored()));
                    } else if (field.getType().equals(Class.class)) {
                        Class<?> cls = (Class<?>) value;
                        doc.add(new TextField(key, cls.getCanonicalName(), indexed.stored()));
                    }
                } else if (value instanceof CharSequence) {
                    doc.add(new TextField(key, value.toString(), indexed.stored()));
                }
            } else if (field.isEnumConstant()) {
                String v = ((Enum<?>) value).name();
                doc.add(new TextField(key, v, indexed.stored()));
            } else if (indexed.deep()) {
                Field[] fields = ReflectionHelper.getAllFields(value.getClass());
                if (fields == null) return;
                for (Field f : fields) {
                    Object v = ReflectionHelper.reflectionUtils().getFieldValue(value, field);
                    if (v != null)
                        build(path, doc, f, v);
                }
            }
        }
    }

    private Indexer<?> getIndexer(Class<? extends Indexer<?>> type) throws Exception {
        if (!type.equals(NullIndexer.class)) {
            Indexer<?> indexer = indexers.get(type.getCanonicalName());
            if (indexer == null) {
                indexer = type.getDeclaredConstructor().newInstance();
                indexers.put(type.getCanonicalName(), indexer);
            }
            return indexer;
        }
        return null;
    }
}
