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

package io.zyient.base.core.services.test;

import io.zyient.base.common.model.services.EResponseState;
import io.zyient.base.common.model.services.ServiceResponse;
import io.zyient.base.core.services.test.model.DemoEntity;
import io.zyient.base.core.services.test.model.DemoEntityServiceResponse;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController(value = "ws")
public class DemoEntityService {
    private final Map<String, DemoEntity> cache = new HashMap<>();

    @RequestMapping(value = "/test/demo/create", method = RequestMethod.PUT)
    public ResponseEntity<DemoEntityServiceResponse> create(@RequestBody DemoEntity entity) {
        try {
            cache.put(entity.entityKey().stringKey(), entity);
            return new ResponseEntity<>(new DemoEntityServiceResponse(EResponseState.Success, entity), HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new DemoEntityServiceResponse(t), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/test/demo/update", method = RequestMethod.POST)
    public ResponseEntity<DemoEntityServiceResponse> update(@RequestBody DemoEntity entity) {
        try {
            if (!cache.containsKey(entity.entityKey().stringKey())) {
                throw new NotFoundException(String.format("Entity not found. [key=%s]", entity.entityKey().getKey()));
            }
            cache.put(entity.entityKey().stringKey(), entity);
            return new ResponseEntity<>(new DemoEntityServiceResponse(EResponseState.Success, entity), HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new DemoEntityServiceResponse(t), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/test/demo/delete/{key}", method = RequestMethod.DELETE)
    public ResponseEntity<ServiceResponse<Boolean>> delete(@PathParam("key") String key) {
        try {
            if (!cache.containsKey(key)) {
                return new ResponseEntity<>(new ServiceResponse<>(EResponseState.Success, false), HttpStatus.OK);
            }
            cache.remove(key);
            return new ResponseEntity<>(new ServiceResponse<>(EResponseState.Success, true), HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new ServiceResponse<>(t), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/test/demo/find", method = RequestMethod.GET)
    public ResponseEntity<DemoEntityServiceResponse> find(@QueryParam("key") String key) {
        try {
            if (!cache.containsKey(key)) {
                throw new NotFoundException(String.format("Entity not found. [key=%s]", key));
            }
            DemoEntity entity = cache.get(key);
            return new ResponseEntity<>(new DemoEntityServiceResponse(EResponseState.Success, entity), HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new DemoEntityServiceResponse(t), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
