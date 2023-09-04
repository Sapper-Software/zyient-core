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

package io.zyient.intake.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public enum MimeTypes {
    AA("audio/audible", "aa"),
    AAC("audio/aac", "aac"),
    AMV("video/x-amv", "amv"),
    APPCACHE("text/cache-manifest", "appcache"),
    APPLICATION_OCTET_STREAM("application/octet-stream", "bin"),
    AVI("video/avi", "avi"),
    BMP("image/bmp", "bmp"),
    CSS("text/css", "css"),
    CSV("text/csv", "csv"),
    DOCX("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "docx"),
    DOTX("application/vnd.openxmlformats-officedocument.wordprocessingml.template", "dotx"),
    EML("message/rfc822", "eml"),
    FLV("video/x-flv", "flv"),
    GIF("image/gif", "gif"),
    GZ("application/gzip", "gz"),
    HTML("text/html", "html"),
    HTM("text/html", "htm"),
    ICO("image/x-icon", "ico"),
    ICS("text/calendar", "ics"),
    IFB("text/calendar", "ifb"),
    JAR("application/java-archive", "jar"),
    JPG("image/jpeg", "jpg"),
    JPE("image/jpeg", "jpe"),
    JPEG("image/jpeg", "jpeg"),
    JS("application/javascript", "js"),
    JSON("application/json", "json"),
    JSONML("application/jsonml+json", "jsonml"),
    LOG("text/x-log", "log"),
    M2V("video/mpeg", "m2v"),
    MIME("message/rfc822", "mime"),
    MKV("video/x-matroska", "mkv"),
    MOV("video/quicktime", "mov"),
    MP3("audio/mpeg", "mp3"),
    MP4("video/mp4", "mp4"),
    MPG("video/mpeg", "mpg"),
    MSG("application/vnd.ms-outlook", "msg"),
    M4P("audio/mp4a-latm", "m4p"),
    OGA("audio/ogg", "oga"),
    OGV("video/ogg", "ogv"),
    ONEPKG("application/onenote", "onepkg"),
    ONETMP("application/onenote", "onetmp"),
    ONETOC("application/onenote", "onetoc"),
    ONETOC2("application/onenote", "onetoc2"),
    PDF("application/pdf", "pdf"),
    PNG("image/png", "png"),
    POTX("application/vnd.openxmlformats-officedocument.presentationml.template", "potx"),
    PPSX("application/vnd.openxmlformats-officedocument.presentationml.slideshow", "ppsx"),
    PPTX("application/vnd.openxmlformats-officedocument.presentationml.presentation", "pptx"),
    RSS("application/rss+xml", "rss"),
    SLDX("application/vnd.openxmlformats-officedocument.presentationml.slide", "sldx"),
    SVG("image/svg+xml", "svg"),
    THMX("application/vnd.openxmlformats-officedocument.presentationml.presentation", "thmx"),
    TIF("image/tiff", "tif"),
    TIFF("image/tiff", "tiff"),
    TXT("text/plain", "txt"),
    VCARD("text/vcard", "vcard"),
    VCF("text/x-vcard", "vcf"),
    VCS("text/x-vcalendar", "vcs"),
    WEBM("video/webm", "webm"),
    WOFF("application/font-woff", "woff"),
    XLSX("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"),
    XLTX("application/vnd.openxmlformats-officedocument.spreadsheetml.template", "xltx"),
    XML("text/xml", "xml"),
    ZIP("application/zip", "zip");

    private final String m_type;
    private final String m_fileExtension;

    private MimeTypes(String type, String fileExtension) {
        this.m_type = type;
        this.m_fileExtension = fileExtension;
    }

    public String getType() {
        return this.m_type;
    }

    public String getFileExtension() {
        return this.m_fileExtension;
    }

    public static MimeTypes findByFileExtension(String fileExtension) {
        MimeTypes[] var4;
        int var3 = (var4 = values()).length;

        for(int var2 = 0; var2 < var3; ++var2) {
            MimeTypes mimeType = var4[var2];
            if (mimeType.getFileExtension().equalsIgnoreCase(fileExtension)) {
                return mimeType;
            }
        }

        return null;
    }

    public static MimeTypes convertToMimeType(String mimeTypeText) {
        MimeTypes[] var4;
        int var3 = (var4 = values()).length;

        for(int var2 = 0; var2 < var3; ++var2) {
            MimeTypes mimeType = var4[var2];
            if (mimeType.getType().equals(mimeTypeText)) {
                return mimeType;
            }
        }

        return null;
    }

    public static Collection<MimeTypes> getCommonImageTypes() {
        return Arrays.asList(getCommonImageTypesAsArray());
    }

    public static MimeTypes[] getCommonImageTypesAsArray() {
        return new MimeTypes[]{BMP, GIF, JPG, JPE, JPEG, PNG, SVG, TIF, TIFF};
    }

    public static Collection<MimeTypes> getCommonVideoTypes() {
        return Arrays.asList(getCommonVideoTypesAsArray());
    }

    public static MimeTypes[] getCommonVideoTypesAsArray() {
        return new MimeTypes[]{AVI, M2V, MKV, MOV, MP4, MPG};
    }

    public static boolean isOneOf(Collection<MimeTypes> mimeTypes, String input) {
        Iterator var3 = mimeTypes.iterator();

        while(var3.hasNext()) {
            MimeTypes mimeType = (MimeTypes)var3.next();
            if (mimeType.getType().equals(input)) {
                return true;
            }
        }

        return false;
    }
}