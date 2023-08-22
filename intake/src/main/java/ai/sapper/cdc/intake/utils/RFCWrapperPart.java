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

package ai.sapper.cdc.intake.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import jakarta.activation.DataHandler;
import jakarta.mail.Header;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Part;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;

public class RFCWrapperPart implements Part {
    private final Part m_rfcPart;
    private final String m_fileName;

    public RFCWrapperPart(Part part, String fileName) {
        this.m_rfcPart = (Part) Preconditions.checkNotNull(part);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileName));
        this.m_fileName = fileName;
    }

    public String getFileName() throws MessagingException {
        return this.m_fileName;
    }

    public void addHeader(String headerName, String headerValue) throws MessagingException {
        this.m_rfcPart.addHeader(headerName, headerValue);
    }

    public Enumeration<Header> getAllHeaders() throws MessagingException {
        return this.m_rfcPart.getAllHeaders();
    }

    public Object getContent() throws IOException, MessagingException {
        return this.m_rfcPart.getContent();
    }

    public String getContentType() throws MessagingException {
        return this.m_rfcPart.getContentType();
    }

    public DataHandler getDataHandler() throws MessagingException {
        return this.m_rfcPart.getDataHandler();
    }

    public String getDescription() throws MessagingException {
        return this.m_rfcPart.getDescription();
    }

    public String getDisposition() throws MessagingException {
        return this.m_rfcPart.getDisposition();
    }

    public String[] getHeader(String headerName) throws MessagingException {
        return this.m_rfcPart.getHeader(headerName);
    }

    public InputStream getInputStream() throws IOException, MessagingException {
        return this.m_rfcPart.getInputStream();
    }

    public int getLineCount() throws MessagingException {
        return this.m_rfcPart.getLineCount();
    }

    public Enumeration<Header> getMatchingHeaders(String[] headerNames) throws MessagingException {
        return this.m_rfcPart.getMatchingHeaders(headerNames);
    }

    public Enumeration<Header> getNonMatchingHeaders(String[] headerNames) throws MessagingException {
        return this.m_rfcPart.getNonMatchingHeaders(headerNames);
    }

    public int getSize() throws MessagingException {
        return this.m_rfcPart.getSize();
    }

    public boolean isMimeType(String mimeType) throws MessagingException {
        return this.m_rfcPart.isMimeType(mimeType);
    }

    public void removeHeader(String headerName) throws MessagingException {
        this.m_rfcPart.removeHeader(headerName);
    }

    public void setContent(Multipart mp) throws MessagingException {
        this.m_rfcPart.setContent(mp);
    }

    public void setContent(Object obj, String type) throws MessagingException {
        this.m_rfcPart.setContent(obj, type);
    }

    public void setDataHandler(DataHandler dh) throws MessagingException {
        this.m_rfcPart.setDataHandler(dh);
    }

    public void setDescription(String description) throws MessagingException {
        this.m_rfcPart.setDescription(description);
    }

    public void setDisposition(String disposition) throws MessagingException {
        this.m_rfcPart.setDisposition(disposition);
    }

    public void setFileName(String filename) throws MessagingException {
        this.m_rfcPart.setFileName(filename);
    }

    public void setHeader(String headerName, String headerValue) throws MessagingException {
        this.m_rfcPart.setHeader(headerName, headerValue);
    }

    public void setText(String text) throws MessagingException {
        this.m_rfcPart.setText(text);
    }

    public void writeTo(OutputStream os) throws IOException, MessagingException {
        this.m_rfcPart.writeTo(os);
    }
}