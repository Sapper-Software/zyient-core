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

package io.zyient.core.extraction.model;

import com.github.pemistahl.lingua.api.Language;
import lombok.NonNull;

public enum LanguageCode {
    AFRIKAANS(Language.AFRIKAANS, "afr"),

    ALBANIAN(Language.ALBANIAN, "sqi"),

    ARABIC(Language.ARABIC, "ara"),

    ARMENIAN(Language.ARMENIAN, "hye"),

    AZERBAIJANI(Language.AZERBAIJANI, "aze"),

    BASQUE(Language.BASQUE, "eus"),

    BELARUSIAN(Language.BELARUSIAN, "bel"),

    BENGALI(Language.BENGALI, "ben"),

    BOKMAL(Language.BOKMAL, ""),

    BOSNIAN(Language.BOSNIAN, "bos"),

    BULGARIAN(Language.BULGARIAN, "bul"),

    CATALAN(Language.CATALAN, "cat"),

    CHINESE(Language.CHINESE, "chi_tra"),

    CROATIAN(Language.CROATIAN, "hrv"),

    CZECH(Language.CZECH, "ces"),

    DANISH(Language.DANISH, "dan"),

    DUTCH(Language.DUTCH, "nld"),

    ENGLISH(Language.ENGLISH, "eng"),

    ESPERANTO(Language.ESPERANTO, "epo"),

    ESTONIAN(Language.ESTONIAN, "est"),

    FINNISH(Language.FINNISH, "fin"),

    FRENCH(Language.FRENCH, "fra"),

    GANDA(Language.GANDA, ""),

    GEORGIAN(Language.GEORGIAN, ""),

    GERMAN(Language.GERMAN, "deu"),

    GREEK(Language.GREEK, "ell"),

    GUJARATI(Language.GUJARATI, "guj"),

    HEBREW(Language.HEBREW, "heb"),

    HINDI(Language.HINDI, "hin"),

    HUNGARIAN(Language.HUNGARIAN, "hun"),

    ICELANDIC(Language.ICELANDIC, "isl"),

    INDONESIAN(Language.INDONESIAN, "ind"),

    IRISH(Language.IRISH, "gle"),

    ITALIAN(Language.ITALIAN, "ita"),

    JAPANESE(Language.JAPANESE, "jpn"),

    KAZAKH(Language.KAZAKH, "kaz"),

    KOREAN(Language.KOREAN, "kor"),

    LATIN(Language.LATIN, "lat"),

    LATVIAN(Language.LATVIAN, "lav"),

    LITHUANIAN(Language.LITHUANIAN, "lit"),

    MACEDONIAN(Language.MACEDONIAN, "mkd"),

    MALAY(Language.MALAY, "msa"),

    MAORI(Language.MAORI, "mri"),

    MARATHI(Language.MARATHI, "mar"),

    MONGOLIAN(Language.MONGOLIAN, "mon"),

    NYNORSK(Language.NYNORSK, ""),

    PERSIAN(Language.PERSIAN, "fas"),

    POLISH(Language.POLISH, "pol"),

    PORTUGUESE(Language.PORTUGUESE, "por"),

    PUNJABI(Language.PUNJABI, "pan"),

    ROMANIAN(Language.ROMANIAN, "ron"),

    RUSSIAN(Language.RUSSIAN, "rus"),

    SERBIAN(Language.SERBIAN, "srp"),

    SHONA(Language.SHONA, ""),

    SLOVAK(Language.SLOVAK, "slk"),

    SLOVENE(Language.SLOVENE, "slv"),

    SOMALI(Language.SOMALI, ""),

    SOTHO(Language.SOTHO, ""),

    SPANISH(Language.SPANISH, "spa"),

    SWAHILI(Language.SWAHILI, "swa"),

    SWEDISH(Language.SWEDISH, "swe"),

    TAGALOG(Language.TAGALOG, "tgl"),

    TAMIL(Language.TAMIL, "tam"),

    TELUGU(Language.TELUGU, "tel"),

    THAI(Language.THAI, "tha"),

    TSONGA(Language.TSONGA, ""),

    TSWANA(Language.TSWANA, "ton"),

    TURKISH(Language.TURKISH, "tur"),

    UKRAINIAN(Language.UKRAINIAN, "ukr"),

    URDU(Language.URDU, "urd"),

    VIETNAMESE(Language.VIETNAMESE, "vie"),

    WELSH(Language.WELSH, "cym"),

    XHOSA(Language.XHOSA, ""),

    YORUBA(Language.YORUBA, "yor"),

    ZULU(Language.ZULU, ""),

    UNKNOWN(Language.UNKNOWN, "");

    private String tesseractLang;
    private Language language;

    LanguageCode(Language language,
                 String tesseractLang) {
        this.tesseractLang = tesseractLang;
        this.language = language;
    }

    public String getTesseractLang() {
        return tesseractLang;
    }

    public Language getLanguage() {
        return language;
    }

    public static LanguageCode from(@NonNull Language language) {
        return LanguageCode.valueOf(language.name());
    }
}
