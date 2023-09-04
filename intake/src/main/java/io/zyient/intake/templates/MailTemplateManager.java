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

package io.zyient.intake.templates;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import freemarker.template.Template;
import io.zyient.intake.model.MailTemplate;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.velocity.app.VelocityEngine;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class MailTemplateManager {
    public static final String CONFIG_NODE_READERS = "readers";

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final Map<String, MailTemplate> templates = new HashMap<>();
    @Setter(AccessLevel.NONE)
    private MailExtensionManager setupManager;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final Map<String, Template> loadedTemplates = new ConcurrentHashMap<>();

    public MailTemplateManager withManager(@Nonnull MailExtensionManager setupManager) {
        this.setupManager = setupManager;
        return this;
    }

    public MailTemplate getTemplate(@Nonnull String name) throws DataStoreException {
        try {
            if (!templates.containsKey(name)) {
                synchronized (templates) {
                    MailTemplate template = setupManager.readTemplate(name);
                    if (template != null) {
                        templates.put(name, template);
                    }
                }
            }
            return templates.get(name);
        } catch (DataStoreException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
        Preconditions.checkArgument(node instanceof ConfigPathNode);
        try {
            AbstractConfigNode cnode = node.find(CONFIG_NODE_READERS);
            if (cnode == null) {
                throw new ConfigurationException("No template readers defined.");
            }
            if (cnode instanceof ConfigPathNode) {
                AbstractConfigNode tnode = ConfigUtils.getPathNode(AbstractContentReader.class, (ConfigPathNode) cnode);
                if (tnode == null) {
                    throw new ConfigurationException("No template readers defined.");
                }
                readTemplateReaders((ConfigPathNode) tnode);
            } else if (cnode instanceof ConfigListElementNode) {
                ConfigListElementNode lnode = (ConfigListElementNode) cnode;
                List<ConfigElementNode> nodes = lnode.getValues();
                for (ConfigElementNode en : nodes) {
                    readTemplateReaders((ConfigPathNode) en);
                }
            }
            velocityEngine = new VelocityEngine();
            velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "file");
            velocityEngine.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, "/");
            velocityEngine.init();

        } catch (ConfigurationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void readTemplateReaders(ConfigPathNode node) throws ConfigurationException {
        String cname = ConfigUtils.getClassAttribute(node);
        if (Strings.isNullOrEmpty(cname)) {
            throw new ConfigurationException(String.format("Class attribute not found. [path=%s]", node.getAbsolutePath()));
        }
        try {
            Class<? extends AbstractContentReader> cls = (Class<? extends AbstractContentReader>) Class.forName(cname);
            AbstractContentReader reader = TypeUtils.createInstance(cls);
            reader.configure(node);

            readers.put(reader.type(), reader);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }


    public String process(@Nonnull MailTemplate template, Map<String, String> params) throws TemplateException {
        switch (template.getTemplateType()) {
            case REGEX:
                return processRegexTemplate(template, params);
            case VELOCITY:
                return processVelocityTemplate(template, params);
        }
        throw new TemplateException(
                String.format("Template type not supported. [type=%s]", template.getTemplateType().name()));
    }

    private String processVelocityTemplate(@Nonnull MailTemplate template, Map<String, String> params) throws TemplateException {
        VelocityContext context = getContext(template, params);
        StringWriter writer = new StringWriter();
        Template templ = getTemplate(template.getTemplate(), template.getTemplateSource());
        if (templ == null) {
            throw new TemplateException(
                    String.format("Failed to load template. [type=%s][name=%s]",
                            template.getTemplateSource().name(), template.getTemplate()));
        }
        templ.merge(context, writer);
        return writer.toString();
    }

    public Template getTemplate(@Nonnull String name, @Nonnull EContentSource source) throws TemplateException {
        if (!loadedTemplates.containsKey(name)) {
            synchronized (loadedTemplates) {
                try {
                    AbstractContentReader reader = readers.get(source);
                    if (reader == null) {
                        throw new TemplateException(String.format("No reader defined for source type. [source=%s]", source.name()));
                    }
                    File tf = reader.read(name);
                    if (tf == null || !tf.exists()) {
                        throw new TemplateException(String.format("failed to read template. [source=%s][name=%s]", source.name(), name));
                    }
                    Template template = velocityEngine.getTemplate(tf.getAbsolutePath(), StandardCharsets.UTF_8.name());
                    if (template != null) {
                        loadedTemplates.put(name, template);
                    } else {
                        throw new TemplateException(String.format("failed to read template. [source=%s][name=%s]", source.name(), name));
                    }
                } catch (IOException ex) {
                    throw new TemplateException(ex);
                }
            }
        }
        return loadedTemplates.get(name);
    }

    private String processRegexTemplate(@Nonnull MailTemplate template, Map<String, String> params) throws TemplateException {
        throw new TemplateException("Method not implemented.");
    }

    private VelocityContext getContext(MailTemplate template, Map<String, String> params) throws TemplateException {
        VelocityContext context = new VelocityContext();
        for (MailTemplateParam param : template.getParams()) {
            String value = params.get(param.getId().getKey());
            if (Strings.isNullOrEmpty(value)) {
                value = param.getDefaultValue();
            }
            if (Strings.isNullOrEmpty(value) && param.isMandatory()) {
                throw new TemplateException(
                        String.format("Missing mandatory parameter : [name=%s]", param.getId().getKey()));
            }
        }
        for (String key : params.keySet()) {
            context.put(key, params.get(key));
        }
        return context;
    }
}
