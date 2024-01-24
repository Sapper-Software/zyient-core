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

package io.zyient.core.extraction.utils;

import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.extraction.model.LanguageCode;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class LanguageUtils {
    private static LanguageDetector detector;
    private static final List<Language> supported = new ArrayList<>();

    private static final ReentrantLock initLock = new ReentrantLock();

    public static LanguageDetector get() throws Exception {
        initLock.lock();
        try {
            if (detector == null) {
                if (!supported.isEmpty()) {
                    detector = LanguageDetectorBuilder.fromAllLanguages()
                            .build();
                } else {
                    detector = LanguageDetectorBuilder.fromLanguages(supported.toArray(new Language[0]))
                            .build();
                }
            }
            return detector;
        } finally {
            initLock.unlock();
        }
    }

    public static void addSupport(@NonNull Language... languages) {
        supported.addAll(Arrays.asList(languages));
    }

    public static LanguageCode detect(@NonNull String text) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(text));
        LanguageDetector detector = get();
        Language language = detector.detectLanguageOf(text);
        DefaultLogger.info(String.format("Detected text language: [%s]", language.name()));
        return LanguageCode.from(language);
    }
}
