/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.filesystem.sync.azure.model;

import com.azure.storage.blob.changefeed.models.BlobChangefeedEvent;
import com.azure.storage.blob.changefeed.models.BlobChangefeedEventData;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AzureFSEvent {
    public static final String SUBJECT_REGEX = "/blobServices/default/containers/(\\w+)/blobs/(.+)";
    public static final Pattern SUBJECT_PATTERN = Pattern.compile(SUBJECT_REGEX);


    private String id;
    private String topic;
    private String subject;
    private AzureFSEventType type;
    private String blobType;
    private String contentType;
    private String sequencer;
    private long timestamp;
    private long size;
    private String container;
    private String path;

    public AzureFSEvent postLoad() throws Exception {
        Matcher m = SUBJECT_PATTERN.matcher(subject);
        if (m.find()) {
            container = m.group(1);
            path = m.group(2);
        }
        if (Strings.isNullOrEmpty(container)) {
            throw new Exception(String.format("Container not found. [subject=%s]", subject));
        }
        if (Strings.isNullOrEmpty(path)) {
            throw new Exception(String.format("Path not found. [subject=%s]", subject));
        }
        return this;
    }

    public static AzureFSEvent parse(@NonNull BlobChangefeedEvent source) throws Exception {
        AzureFSEvent event = new AzureFSEvent();
        event.id = source.getId();
        event.topic = source.getTopic();
        event.subject = source.getSubject();
        event.type = AzureFSEventType.from(source.getEventType());
        event.timestamp = source.getEventTime().toEpochSecond();
        BlobChangefeedEventData data = source.getData();
        if (data != null) {
            event.blobType = data.getBlobType().name();
            event.contentType = data.getContentType();
            event.size = data.getContentLength();
            event.sequencer = data.getSequencer();
        } else if (event.type != AzureFSEventType.Ignore) {
            throw new Exception(String.format("Event has not data. [id=%s][type=%s]",
                    source.getId(), event.type.name()));
        }
        event.postLoad();
        return event;
    }
}
