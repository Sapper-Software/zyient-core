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

package io.zyient.core.persistence.impl.solr.schema;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.Setter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.lang.reflect.Field;

@Getter
@Setter
public class SolrSchemaGenerator {
    public static final String __TEMPLATE_RESOURCE = "solr/schema-template.xml";
    public static final String __TEMPLATE_PATH = "/schema/template";

    public static class Constants {
        public static final String INDEX_NODE_NAME = "field";
        public static final String INDEX_ATTR_NAME = "name";
        public static final String INDEX_ATTR_TYPE = "type";
        public static final String INDEX_ATTR_INDEXED = "indexed";
        public static final String INDEX_ATTR_STORED = "stored";
    }

    @Parameter(names = {"--type", "-t"}, required = true, description = "Entity Class (canonical class name)")
    private String type;
    @Parameter(names = {"--dir", "-o"}, description = "Output directory. [default=pwd]")
    private String outDir;


    public void run() throws Exception {
        if (Strings.isNullOrEmpty(outDir)) {
            outDir = ".";
        }
        File dir = new File(outDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(String.format("Failed to create output directory. [path=%s]",
                        dir.getAbsolutePath()));
            }
        }
        Class<?> cls = Class.forName(type);
        String name = cls.getSimpleName();
        String path = PathUtils.formatPath(String.format("%s/%s.schema.xml", dir.getAbsolutePath(), name));
        File outf = new File(path);
        if (outf.exists()) {
            outf.delete();
        }
        try (InputStream stream
                     = Thread.currentThread().getContextClassLoader().getResourceAsStream(__TEMPLATE_RESOURCE)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(stream);
            doc.getDocumentElement().normalize();
            XPath xPath = XPathFactory.newInstance().newXPath();
            Object n = xPath.compile(__TEMPLATE_PATH).evaluate(doc, XPathConstants.NODE);
            if (!(n instanceof Node toDelete)) {
                throw new Exception(String.format("Invalid template: template node not found. [path=%s]",
                        __TEMPLATE_PATH));
            }
            Node schema = toDelete.getParentNode();
            Field[] fields = ReflectionHelper.getAllFields(cls);
            if (fields != null) {
                for (Field field : fields) {
                    process(field, doc, schema, null);
                }
            }
            schema.removeChild(toDelete);
            try (FileOutputStream fos = new FileOutputStream(outf)) {
                writeXml(doc, fos);
            }
        }
    }

    private static void writeXml(Document doc,
                                 OutputStream output)
            throws TransformerException {

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(output);
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.transform(source, result);
    }

    private void process(Field field, Document doc, Node parent, String path) throws Exception {
        if (field.isAnnotationPresent(org.apache.solr.client.solrj.beans.Field.class)) {
            org.apache.solr.client.solrj.beans.Field f
                    = field.getAnnotation(org.apache.solr.client.solrj.beans.Field.class);
            SolrFieldTypes type = SolrFieldTypes.getType(field);
            Element node = doc.createElement(Constants.INDEX_NODE_NAME);
            String fname = f.value();
            if (Strings.isNullOrEmpty(fname)) {
                fname = field.getName().toLowerCase();
            }
            node.setAttribute(Constants.INDEX_ATTR_NAME, fname);
            node.setAttribute(Constants.INDEX_ATTR_TYPE, type.type());
            node.setAttribute(Constants.INDEX_ATTR_INDEXED, "true");
            node.setAttribute(Constants.INDEX_ATTR_STORED, "true");
            parent.appendChild(node);
        }
    }

    public static void main(String[] args) {
        try {
            SolrSchemaGenerator schemaGenerator = new SolrSchemaGenerator();
            JCommander.newBuilder().addObject(schemaGenerator).build().parse(args);
            schemaGenerator.run();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(t.getLocalizedMessage());
            t.printStackTrace();
        }
    }
}
