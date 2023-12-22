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

package io.zyient.core.filesystem.sync.s3.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3Event {
    public static final String KEY_RECORDS = "Records";
    public static final String KEY_REGION = "awsRegion";
    public static final String KEY_PRINCIPAL_ID = "userIdentity.principalId";
    public static final String KEY_BUCKET = "s3.bucket";
    public static final String KEY_OBJECT = "s3.object";
    public static final String KEY_NAME = "eventName";
    public static final String KEY_TIMESTAMP = "eventTime";

    private String messageId;
    private String region;
    private String principalId;
    private DateTime timestamp;
    private S3EventName name;
    private S3EventBucket bucket;
    private S3EventObject data;

    @SuppressWarnings("unchecked")
    public static S3Event from(@NonNull Map<String, Object> data) throws Exception {
        S3Event event = new S3Event();
        event.region = (String) data.get(KEY_REGION);
        if (Strings.isNullOrEmpty(event.region)) {
            throw new Exception(String.format("Required field not found. [name=%s]", KEY_REGION));
        }
        event.principalId = (String) JSONUtils.find(data, KEY_PRINCIPAL_ID);
        String name = (String) data.get(KEY_NAME);
        if (Strings.isNullOrEmpty(name)) {
            throw new Exception("S3 Event name not found...");
        }
        event.name = S3EventName.from(name);
        String timestamp = (String) data.get(KEY_TIMESTAMP);
        if (Strings.isNullOrEmpty(timestamp)) {
            throw new Exception("S3 Event timestamp not found...");
        }
        DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
        event.timestamp = parser.parseDateTime(timestamp);
        Map<String, Object> bucket = (Map<String, Object>) JSONUtils.find(data, KEY_BUCKET);
        if (bucket == null) {
            throw new Exception("S3 Bucket node not found...");
        }
        event.bucket = S3EventBucket.from(bucket);
        Map<String, Object> object = (Map<String, Object>) JSONUtils.find(data, KEY_OBJECT);
        if (object == null) {
            throw new Exception("S3 Object node not found...");
        }
        event.data = S3EventObject.from(object);
        return event;
    }

    @SuppressWarnings("unchecked")
    public static List<S3Event> read(Map<String, Object> data) throws Exception {
        if (data.containsKey(KEY_RECORDS)) {
            Object records = data.get(KEY_RECORDS);
            if (records instanceof Collection<?> values) {
                List<S3Event> events = new ArrayList<>();
                for (Object record : values) {
                    if (record instanceof Map<?,?>) {
                        S3Event event = S3Event.from((Map<String, Object>) record);
                        events.add(event);
                    } else {
                        throw new Exception(String.format("Invalid record type. [type=%s]",
                                record.getClass().getCanonicalName()));
                    }
                }
                return events;
            } else {
                throw new Exception(String.format("Invalid records type. [type=%s]",
                        records.getClass().getCanonicalName()));
            }
        }
        return null;
    }
}
