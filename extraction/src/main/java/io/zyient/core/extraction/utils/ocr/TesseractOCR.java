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
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.extraction.ExtractionConvertor;
import io.zyient.core.extraction.model.*;
import io.zyient.core.extraction.utils.CellDetector;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.sourceforge.tess4j.ITessAPI;
import net.sourceforge.tess4j.TessAPI1;
import net.sourceforge.tess4j.util.ImageIOHelper;
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
import org.opencv.highgui.HighGui;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import static net.sourceforge.tess4j.ITessAPI.TRUE;

@Getter
@Setter
@Accessors(fluent = true)
public class TesseractOCR implements ExtractionConvertor<String>, Closeable {
    static {
        OpenCV.loadLocally();
    }

    private final OCRFileType sourceType;
    private OCRFileType outputType = OCRFileType.Alto;
    private final File sourcePath;
    private final File outputPath;
    private final File dataPath;
    private final String sourceReferenceId;
    private final String sourceUri;
    private LanguageCode language = LanguageCode.ENGLISH;
    private boolean show = false;
    private boolean greyscale = true;
    private File tempDir;
    private ITessAPI.TessBaseAPI handle;

    public TesseractOCR(@NonNull OCRFileType sourceType,
                        @NonNull String sourcePath,
                        @NonNull String outputPath,
                        @NonNull String dataPath,
                        @NonNull String sourceReferenceId,
                        @NonNull String sourceUri) throws Exception {
        this.sourceType = sourceType;
        this.sourcePath = new File(sourcePath);
        this.sourceReferenceId = sourceReferenceId;
        this.sourceUri = sourceUri;
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
        handle = TessAPI1.TessBaseAPICreate();
    }

    private void runWithImage(File input,
                              Source source,
                              DocumentSection doc,
                              int index) throws Exception {
        CellDetector detector = new CellDetector();
        File outf = input;
        if (greyscale) {
            outf = convertToGreyscale(input, detector);
        } else {
            Mat src = Imgcodecs.imread(outf.getAbsolutePath());
            detector.image(src);
        }
        if (doc == null) {
            doc = source.create(0);
            index = 0;
        }
        runOcr(outf.getAbsolutePath(),
                language.getTesseractLang(),
                doc,
                detector,
                index);
        if (show) {
            showOutput(detector);
        }
    }

    private void runOcr(String path,
                        String language,
                        DocumentSection doc,
                        CellDetector detector,
                        int index) throws Exception {
        Page page = doc.add(index);
        File imgfile = new File(path);
        BufferedImage image = ImageIO.read(new FileInputStream(imgfile));
        ByteBuffer buf = ImageIOHelper.convertImageData(image);
        int bpp = image.getColorModel().getPixelSize();
        int bytespp = bpp / 8;
        int bytespl = (int) Math.ceil(image.getWidth() * bpp / 8.0);
        page.setPixelSize(bpp);
        page.setBoundingBox(new BoundingBox()
                .start(0, 0)
                .end(image.getWidth(), image.getHeight()));
        TessAPI1.TessBaseAPIInit3(handle, dataPath.getAbsolutePath(), language);
        TessAPI1.TessBaseAPISetPageSegMode(handle, ITessAPI.TessPageSegMode.PSM_AUTO);
        TessAPI1.TessBaseAPISetImage(handle, buf, image.getWidth(), image.getHeight(), bytespp, bytespl);
        ITessAPI.ETEXT_DESC monitor = new ITessAPI.ETEXT_DESC();
        ITessAPI.TimeVal timeout = new ITessAPI.TimeVal();
        timeout.tv_sec = new NativeLong(0L); // time > 0 causes blank ouput
        monitor.end_time = timeout;
        TessAPI1.TessBaseAPIRecognize(handle, monitor);
        ITessAPI.TessResultIterator ri = TessAPI1.TessBaseAPIGetIterator(handle);
        ITessAPI.TessPageIterator pi = TessAPI1.TessResultIteratorGetPageIterator(ri);
        TessAPI1.TessPageIteratorBegin(pi);
        int level = ITessAPI.TessPageIteratorLevel.RIL_WORD;
        int offset = 0;
        do {
            Pointer ptr = TessAPI1.TessResultIteratorGetUTF8Text(ri, level);
            String word = ptr.getString(0);
            TessAPI1.TessDeleteText(ptr);
            float confidence = TessAPI1.TessResultIteratorConfidence(ri, level);
            IntBuffer leftB = IntBuffer.allocate(1);
            IntBuffer topB = IntBuffer.allocate(1);
            IntBuffer rightB = IntBuffer.allocate(1);
            IntBuffer bottomB = IntBuffer.allocate(1);
            TessAPI1.TessPageIteratorBoundingBox(pi, level, leftB, topB, rightB, bottomB);
            int left = leftB.get();
            int top = topB.get();
            int right = rightB.get();
            int bottom = bottomB.get();
            DefaultLogger.debug(String.format("%s %d %d %d %d %f", word, left, top, right, bottom, confidence));
            BoundingBox box = new BoundingBox();
            box.start(left, top)
                    .end(right, bottom)
                    .setPage(page.getNumber());
            IntBuffer boldB = IntBuffer.allocate(1);
            IntBuffer italicB = IntBuffer.allocate(1);
            IntBuffer underlinedB = IntBuffer.allocate(1);
            IntBuffer monospaceB = IntBuffer.allocate(1);
            IntBuffer serifB = IntBuffer.allocate(1);
            IntBuffer smallcapsB = IntBuffer.allocate(1);
            IntBuffer pointSizeB = IntBuffer.allocate(1);
            IntBuffer fontIdB = IntBuffer.allocate(1);
            String fontName = TessAPI1.TessResultIteratorWordFontAttributes(ri, boldB, italicB, underlinedB,
                    monospaceB, serifB, smallcapsB, pointSizeB, fontIdB);
            boolean bold = boldB.get() == TRUE;
            boolean italic = italicB.get() == TRUE;
            boolean underlined = underlinedB.get() == TRUE;
            boolean monospace = monospaceB.get() == TRUE;
            boolean serif = serifB.get() == TRUE;
            boolean smallcaps = smallcapsB.get() == TRUE;
            int pointSize = pointSizeB.get();
            int fontId = fontIdB.get();

            FontInfo fi = new FontInfo();
            fi.setName(fontName);
            fi.setFontId(fontId);
            fi.setSize(pointSize);
            fi.setBold(bold);
            fi.setItalics(italic);
            fi.setUnderlined(underlined);

            DefaultLogger.debug(String.format("  font: %s, size: %d, font id: %d, bold: %b,"
                            + " italic: %b, underlined: %b, monospace: %b, serif: %b, smallcap: %b", fontName, pointSize,
                    fontId, bold, italic, underlined, monospace, serif, smallcaps));
            TextCell cell = (TextCell) page.add(TextCell.class, offset);
            cell.setData(word);
            cell.setBoundingBox(box);
            cell.setConfidence(confidence);
            cell.setFontInfo(fi);
            detector.decorate(cell);
            offset++;
        } while (TessAPI1.TessPageIteratorNext(pi, level) == TRUE);
        TessAPI1.TessResultIteratorDelete(ri);
    }


    private void showOutput(CellDetector detector) throws Exception {
        Mat img = detector.greyscale();
        if (img == null) {
            img = detector.image();
        }

        HighGui.imshow("Page", img);
        HighGui.waitKey(10000);
        HighGui.destroyAllWindows();
    }

    private void runWithPdf(Source source) throws Exception {
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
            DocumentSection doc = source.create(0);
            for (int ii = 0; ii < count; ii++) {
                String fname = String.format("%s_%d.png", name, ii);
                String path = String.format("%s/%s", dir.getAbsolutePath(), fname);
                int dpi = 300;
                File outf = new File(path);
                BufferedImage bImage = pdfRenderer.renderImageWithDPI(ii, dpi, ImageType.RGB);
                ImageIO.write(bImage, "png", outf);
                runWithImage(outf, source, doc, ii);
            }
        }
    }


    private File convertToGreyscale(File source, CellDetector detector) throws Exception {
        String name = FilenameUtils.getName(source.getAbsolutePath());
        name = FilenameUtils.removeExtension(name);
        String path = String.format("%s/gs-%s.png", tempDir.getAbsolutePath(), name);
        File outf = new File(path);
        if (outf.exists()) {
            outf.delete();
        }
        Mat src = Imgcodecs.imread(source.getAbsolutePath());
        detector.image(src);
        Mat dst = new Mat();
        Imgproc.cvtColor(src, dst, Imgproc.COLOR_RGB2GRAY);
        Imgcodecs.imwrite(outf.getAbsolutePath(), dst);
        detector.greyscale(dst);
        return outf;
    }

    public void run() throws Exception {
        Source src = new Source(sourceReferenceId, sourceUri);
        src.getMetadata().language(language
                .getLanguage()
                .getIsoCode639_3()
                .name());
        tempDir = PathUtils.getTempDir("OCR");
        if (sourceType == OCRFileType.Image) {
            runWithImage(sourcePath, src, null, 0);
        } else if (sourceType == OCRFileType.PDF) {
            runWithPdf(src);
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

    @Override
    public void close() throws IOException {
        if (handle != null) {
            TessAPI1.TessBaseAPIDelete(handle);
        }
    }
}
