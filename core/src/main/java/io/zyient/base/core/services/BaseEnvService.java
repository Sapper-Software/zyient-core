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

import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.services.model.EnvResponse;
import io.zyient.base.core.services.model.EnvStartRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
            }
            return ResponseEntity.ok(from(env));
        } catch (Throwable t) {
            return ResponseEntity.internalServerError()
                    .body(new EnvResponse(request.getName(), state, t));
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
