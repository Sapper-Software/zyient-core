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

package io.zyient.core.mapping.transformers;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.mapper.MappingSettings;
import io.zyient.core.mapping.model.DateMappedElement;
import io.zyient.core.mapping.model.mapping.MappedElement;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.SerializationException;

import java.text.SimpleDateFormat;

@Getter
@Setter
@Accessors(fluent = true)
public class DateIntegerTransformer implements Transformer<Integer> {

    private String name;
    private DateMappedElement dateMappedElement;

    @Override
    public Transformer<Integer> configure(@NonNull MappingSettings settings, @NonNull MappedElement element) throws ConfigurationException {
        Preconditions.checkArgument(element instanceof DateMappedElement);
        dateMappedElement = (DateMappedElement) element;
        return this;
    }

    @Override
    public Integer read(@NonNull Object source) throws SerializationException {
        if (source instanceof String value) {

            SimpleDateFormat df = new SimpleDateFormat(dateMappedElement.getSourceFormat());
            SimpleDateFormat targetFormatDf = new SimpleDateFormat(dateMappedElement.getTargetFormat());
            try {
                return Integer.parseInt(targetFormatDf.format(df.parse(value)));
            } catch (Exception e) {
                DefaultLogger.warn(String.format("Cannot transform to String. [source=%s]", source.getClass()));
                return 0;
            }
        }
        DefaultLogger.warn(String.format("Skipping date transformation. [source=%s]", source.getClass()));
        return 0;
    }
}
