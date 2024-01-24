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

package io.zyient.core.extraction.utils.ocr;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.extraction.ExtractionConvertor;
import io.zyient.core.extraction.model.LanguageCode;
import io.zyient.core.extraction.model.Source;
import io.zyient.core.extraction.utils.LanguageUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.sourceforge.tess4j.*;
import nu.pattern.OpenCV;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.opencv.core.Mat;
import org.opencv.core.Point;
import org.opencv.core.Scalar;
import org.opencv.highgui.HighGui;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class TesseractOCR implements ExtractionConvertor<String> {
    static {
        OpenCV.loadLocally();
    }

    private final OCRFileType sourceType;
    private OCRFileType outputType = OCRFileType.Alto;
    private final File sourcePath;
    private final File outputPath;
    private final File dataPath;
    private LanguageCode language = LanguageCode.ENGLISH;
    private boolean detectLanguage = false;
    private boolean show = false;
    private Tesseract tesseract;
    private File tempDir;
    private List<ITesseract.RenderedFormat> formats = new ArrayList<>();

    public TesseractOCR(@NonNull OCRFileType sourceType,
                        @NonNull String sourcePath,
                        @NonNull String outputPath,
                        @NonNull String dataPath) throws Exception {
        this.sourceType = sourceType;
        this.sourcePath = new File(sourcePath);
        if (!this.sourcePath.exists()) {
            throw new IOException(String.format("Input file not found. [path=%s]", this.sourcePath.getAbsolutePath()));
        }
        this.dataPath = new File(dataPath);
        if (!this.dataPath.exists()) {
            throw new IOException(String.format("Data Path is invalid. [path=%s]", this.dataPath.getAbsolutePath()));
        }
        String name = FilenameUtils.getName(this.sourcePath.getAbsolutePath());
        name = FilenameUtils.removeExtension(name);
        outputPath = String.format("%s/%s", outputPath, name);
        this.outputPath = new File(outputPath);
        if (!this.outputPath.exists()) {
            if (!this.outputPath.mkdirs()) {
                throw new IOException(String.format("Failed to create output folder. [path=%s]",
                        this.outputPath.getAbsolutePath()));
            }
        }
    }

    private void runWithImage(File source) throws Exception {
        File outf = convertToGreyscale(source);
        if (detectLanguage) {
            LanguageCode code = detect(outf);
            if (code != null) {
                String lang = code.getTesseractLang();
                if (!Strings.isNullOrEmpty(lang))
                    tesseract.setLanguage(lang);
                else {
                    DefaultLogger.warn(String.format("Failed to detect language. [file=%s]", source.getAbsolutePath()));
                }
            } else {
                DefaultLogger.warn(String.format("Failed to detect language. [file=%s]", source.getAbsolutePath()));
            }
        }
        String name = FilenameUtils.getName(source.getAbsolutePath());
        name = FilenameUtils.removeExtension(name);
        String path = String.format("%s/%s", outputPath.getAbsolutePath(), name);

        OCRResult result = tesseract.createDocumentsWithResults(outf.getAbsolutePath(),
                path,
                formats,
                ITessAPI.TessPageIteratorLevel.RIL_WORD);
        if (show) {
            showOutput(result, outf);
        }
    }

    private void showOutput(OCRResult result, File source) throws Exception {
        Mat img = Imgcodecs.imread(source.getAbsolutePath());
        for (Word word : result.getWords()) {
            Rectangle rect = word.getBoundingBox();
            Point xy = new Point(rect.x, rect.y);
            Point wh = new Point(rect.x + rect.width, rect.y + rect.height);
            Imgproc.rectangle(img, xy, wh, new Scalar(0, 255, 0));
        }

        HighGui.imshow(source.getName(), img);
        HighGui.waitKey(60000);
    }

    private void runWithPdf() throws Exception {
        try (PDDocument document = Loader.loadPDF(sourcePath)) {
            PDFRenderer pdfRenderer = new PDFRenderer(document);

            String name = FilenameUtils.getName(sourcePath.getAbsolutePath());
            name = FilenameUtils.removeExtension(name);
            int count = document.getNumberOfPages();
            String outd = String.format("%s/%s", tempDir.getAbsolutePath(), name);
            File dir = new File(outd);
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new Exception(String.format("Failed to create temp directory. [path=%s]",
                            dir.getAbsolutePath()));
                }
            }
            for (int ii = 0; ii < count; ii++) {
                String fname = String.format("%s_%d.png", name, ii);
                String path = String.format("%s/%s", dir.getAbsolutePath(), fname);
                int dpi = 300;
                File outf = new File(path);
                BufferedImage bImage = pdfRenderer.renderImageWithDPI(ii, dpi, ImageType.RGB);
                ImageIO.write(bImage, "png", outf);
                runWithImage(outf);
            }
        }
    }

    private LanguageCode detect(File source) throws Exception {
        String output = tesseract.doOCR(source);
        if (!Strings.isNullOrEmpty(output)) {
            return LanguageUtils.detect(output);
        } else {
            DefaultLogger.warn(String.format("OCR output is empty. [path=%s]", source.getAbsolutePath()));
        }
        return null;
    }


    private File convertToGreyscale(File source) throws Exception {
        String name = FilenameUtils.getName(source.getAbsolutePath());
        String path = String.format("%s/gs-%s", tempDir.getAbsolutePath(), name);
        File outf = new File(path);
        if (outf.exists()) {
            outf.delete();
        }
        Mat src = Imgcodecs.imread(source.getAbsolutePath());
        Mat dst = new Mat();
        Imgproc.cvtColor(src, dst, Imgproc.COLOR_RGB2GRAY);
        Imgcodecs.imwrite(outf.getAbsolutePath(), dst);
        return outf;
    }

    public void run() throws Exception {
        tesseract = new Tesseract();
        tesseract.setDatapath(dataPath.getAbsolutePath());
        tesseract.setLanguage(language.getTesseractLang());
        tesseract.setOcrEngineMode(1);
        tesseract.setPageSegMode(1);

        tempDir = PathUtils.getTempDir("OCR");
        formats.add(ITesseract.RenderedFormat.PDF);
        if (outputType == OCRFileType.hOCR) {
            formats.add(ITesseract.RenderedFormat.HOCR);
        } else {
            formats.add(ITesseract.RenderedFormat.ALTO);
        }

        if (sourceType == OCRFileType.Image) {
            runWithImage(sourcePath);
        } else if (sourceType == OCRFileType.PDF) {
            runWithPdf();
        } else {
            throw new Exception(String.format("Invalid source type: [type=%s]", sourceType.name()));
        }
    }

    @Override
    public ExtractionConvertor<String> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                 @NonNull BaseEnv<?> env) throws ConfigurationException {
        return this;
    }

    @Override
    public Source convert(@NonNull String source,
                          @NonNull String sourceReferenceId,
                          @NonNull String sourceUri) throws Exception {
        if (outputPath == null) {
            throw new Exception("OCR not created...");
        }
        if (!outputPath.exists()) {
            throw new IOException(String.format("Input source not found. [path=%s]",
                    outputPath.getAbsolutePath()));
        }
        Source src = new Source(sourceReferenceId, sourceUri);
        src.getMetadata().language(language
                .getLanguage()
                .getIsoCode639_3()
                .name());
        if (outputType == OCRFileType.hOCR) {
            return convertHOcr(outputPath, src);
        } else if (outputType == OCRFileType.Alto) {
            return convertAlto(outputPath, src);
        }
        throw new Exception(String.format("Invalid output type. [type=%s]", outputType.name()));
    }

    private Source convertAlto(File dir, Source source) throws Exception {
        File[] files = dir.listFiles((file, s) -> {
            String name = FilenameUtils.getName(file.getAbsolutePath());
            String ext = FilenameUtils.getExtension(name);
            if (!Strings.isNullOrEmpty(ext)) {
                return ext.compareToIgnoreCase("xml") == 0;
            }
            return false;
        });
        if (files != null) {
            for (File file : files) {
                processAltoXml(file, source);
            }
        }
        return source;
    }

    private void processAltoXml(File file, Source source) throws Exception {

    }

    private Source convertHOcr(File dir, Source source) throws Exception {
        throw new Exception("Method not implemented...");
    }
}
