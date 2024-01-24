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

package io.zyient.base.core.services.test;

import com.google.common.base.Strings;
import io.zyient.base.common.model.services.EResponseState;
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.services.model.FileEntity;
import io.zyient.base.core.services.model.FileEntityServiceResponse;
import io.zyient.base.core.utils.SpringUtils;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.Entity;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@RestController
public class DemoFileService {
    private final Map<String, FileEntity> files = new HashMap<>();

    @RequestMapping(value = "/test/demo/upload",
            method = RequestMethod.POST,
            consumes = {MediaType.MULTIPART_FORM_DATA},
            produces = {MediaType.APPLICATION_JSON})
    public ResponseEntity<FileEntityServiceResponse> uploadPost(@FormDataParam("file") InputStream fileInputStream,
                                                                @FormDataParam("file") FormDataContentDisposition fileMetaData,
                                                                @FormDataParam("entity") FileEntity entity) {
        return uploadPut(fileInputStream, fileMetaData, entity);
    }

    @RequestMapping(value = "/test/demo/upload",
            method = RequestMethod.PUT,
            consumes = {MediaType.MULTIPART_FORM_DATA},
            produces = {MediaType.APPLICATION_JSON})
    public ResponseEntity<FileEntityServiceResponse> uploadPut(@FormDataParam("file") InputStream fileInputStream,
                                                               @FormDataParam("file") FormDataContentDisposition fileMetaData,
                                                               @FormDataParam("entity") FileEntity entity) {
        try {
            File path = PathUtils.getTempFile(String.format("%s_%s",
                    UUID.randomUUID().toString(), fileMetaData.getFileName()));
            try (FileOutputStream fos = new FileOutputStream(path)) {
                int bsize = 4096;
                byte[] buffer = new byte[bsize];
                while (true) {
                    int r = fileInputStream.read(buffer);
                    if (r <= 0) break;
                    fos.write(buffer, 0, r);
                    if (r < bsize) break;
                }
            }
            String checkSum = ChecksumUtils.computeSHA256(path);
            if (!Strings.isNullOrEmpty(entity.getCheckSum())) {
                if (checkSum.compareTo(entity.getCheckSum()) != 0) {
                    throw new IOException(String.format("Checksum failed. [key=%s]", entity.getKey().getKey()));
                }
            }
            entity.setCheckSum(checkSum);
            entity.setPath(path);
            entity.setMimeType(fileMetaData.getType());
            files.put(entity.entityKey().stringKey(), entity);
            return new ResponseEntity<>(new FileEntityServiceResponse(EResponseState.Success, entity), HttpStatus.OK);
        } catch (Exception ex) {
            return new ResponseEntity<>(new FileEntityServiceResponse(ex), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/test/demo/download/{key}",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_OCTET_STREAM})
    public ResponseEntity<StreamingOutput> download(@PathVariable String key) {
        try {
            if (files.containsKey(key)) {
                FileEntity fi = files.get(key);
                StreamingOutput fileStream = new StreamingOutput() {
                    @Override
                    public void write(java.io.OutputStream output) throws IOException, WebApplicationException {
                        try {
                            Path path = Paths.get(fi.getPath().getAbsolutePath());
                            byte[] data = Files.readAllBytes(path);
                            output.write(data);
                            output.flush();
                        } catch (Exception e) {
                            throw new WebApplicationException("File Not Found !!");
                        }
                    }
                };
                String header = String.format("attachment; filename = \"%s\"", fi.getName());
                return ResponseEntity.ok()
                        .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM)
                        .header("content-disposition", header)
                        .body(fileStream);
            }
            throw new IOException(String.format("File Entity not found. [key=%s]", key));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            return ResponseEntity.internalServerError()
                    .body(null);
        }
    }

    @RequestMapping(value = "/test/demo/classes",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON})
    public ResponseEntity<List<String>> getClasses() {
        try {
            // Set<Class<?>> classes = ReflectionHelper.findAllClasses("io.zyient.base.core.services.test.model",
               //     getClass());
            Set<Class<?>> classes = SpringUtils
                    .findAllEntityClassesInPackage("io.zyient.base.core.services.test.model", Entity.class);
            List<String> values = null;
            if (classes != null && !classes.isEmpty()) {
                values = new ArrayList<>(classes.size());
                for (Class<?> cls : classes) {
                    values.add(cls.getCanonicalName());
                }
            } else {
                throw new Exception("No classes found...");
            }
            return ResponseEntity.ok(values);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            return ResponseEntity.internalServerError()
                    .body(null);
        }
    }
}
