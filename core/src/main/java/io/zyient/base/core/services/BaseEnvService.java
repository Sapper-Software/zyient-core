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

package io.zyient.base.core.services;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.services.model.EnvResponse;
import io.zyient.base.core.services.model.EnvShutdownResponse;
import io.zyient.base.core.services.model.EnvStartRequest;
import io.zyient.base.core.services.model.ShutdownStatus;
import org.apache.commons.configuration2.XMLConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@RestController
public class BaseEnvService {

    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/admin/env/start", method = RequestMethod.POST)
    public ResponseEntity<EnvResponse> start(@RequestParam("passwd") String password,
                                             @RequestBody EnvStartRequest request) {
        String state = "ERROR";
        try {
            Class<? extends BaseEnv<?>> type = (Class<? extends BaseEnv<?>>) Class.forName(request.getEnvClass());
            BaseEnv<?> env = BaseEnv.get(request.getName(), type);
            if (env == null) {
                File config = new File(request.getConfigFilePath());
                if (!config.exists()) {
                    throw new IOException(String.format("Configuration file not found. [path=%s]",
                            config.getAbsolutePath()));
                }
                XMLConfiguration cfg = ConfigReader.read(config.getAbsolutePath(), EConfigFileType.File);
                env = BaseEnv.create(type, cfg, password);
            }
            return ResponseEntity.ok(from(env));
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            return ResponseEntity.internalServerError()
                    .body(new EnvResponse(request.getName(), state, t));
        }
    }

    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/admin/env/status/{name}", method = RequestMethod.GET)
    public ResponseEntity<EnvResponse> status(@PathVariable("name") String name,
                                              @RequestParam("type") String type) {
        String state = "ERROR";
        try {
            Class<? extends BaseEnv<?>> t = (Class<? extends BaseEnv<?>>) Class.forName(type);
            BaseEnv<?> env = BaseEnv.get(name, t);
            if (env == null) {
                throw new Exception(String.format("Environment not found. [name=%s][type=%s]",
                        name, type));
            }
            return ResponseEntity.ok(from(env));
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            return ResponseEntity.internalServerError()
                    .body(new EnvResponse(name, state, t));
        }
    }

    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/admin/env/stop/{name}", method = RequestMethod.GET)
    public ResponseEntity<EnvResponse> stop(@PathVariable("name") String name,
                                            @RequestParam("type") String type) {
        String state = "ERROR";
        try {
            Class<? extends BaseEnv<?>> t = (Class<? extends BaseEnv<?>>) Class.forName(type);
            BaseEnv<?> env = BaseEnv.get(name, t);
            if (env == null) {
                throw new Exception(String.format("Environment not found. [name=%s][type=%s]",
                        name, type));
            }
            if (BaseEnv.remove(name)) {
                return ResponseEntity.ok(from(env));
            }
            throw new Exception(String.format("Failed to shutdown environment. [name=%s][type=%s]",
                    name, type));
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            return ResponseEntity.internalServerError()
                    .body(new EnvResponse(name, state, t));
        }
    }

    @RequestMapping(value = "/admin/env/shutdown", method = RequestMethod.GET)
    public ResponseEntity<EnvShutdownResponse> shutdown() {
        EnvShutdownResponse response = new EnvShutdownResponse();
        try {
            Map<String, ShutdownStatus> statuses = BaseEnv.disposeAll();
            response.setResponses(statuses);
            response.setTimestamp(System.currentTimeMillis());
            return ResponseEntity.ok(response);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            response.setTimestamp(System.currentTimeMillis());
            response.setError(t);
            return ResponseEntity.internalServerError()
                    .body(response);
        }
    }

    private EnvResponse from(BaseEnv<?> env) {
        EnvResponse response = new EnvResponse();
        response.setName(env.name());
        response.setInstance(env.moduleInstance());
        response.setState(env.state().getState().name());
        if (env.state().hasError()) {
            response.setError(env.state().getError());
        }
        return response;
    }
}
