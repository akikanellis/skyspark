package com.github.dkanellis.skyspark.performance.result;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import javax.validation.constraints.NotNull;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;

public class XmlResultWriter implements ResultWriter {

    private final String outputFilePath;
    private final Workbook workbook;
    private final Sheet resultSheet;

    public XmlResultWriter(String outputFilePath) {
        this.outputFilePath = checkNotEmpty(outputFilePath);
        workbook = new HSSFWorkbook();
        resultSheet = workbook.createSheet("Results");
        Row row = resultSheet.createRow(resultSheet.getLastRowNum());
        row.createCell(0).setCellValue("Algorithm");
        row.createCell(1).setCellValue("Data Type");
        row.createCell(2).setCellValue("Data Size");
        row.createCell(3).setCellValue("Number of Skylines");
        row.createCell(4).setCellValue("Elapsed Time (ns)");
    }

    @Override
    public void writeResult(@NotNull Result result) {
        Row row = resultSheet.createRow(resultSheet.getLastRowNum() + 1);
        row.createCell(0).setCellValue(result.getAlgorithmName());
        row.createCell(1).setCellValue(result.getDataType());
        row.createCell(2).setCellValue(result.getDataSize());
        row.createCell(3).setCellValue(result.getNumberOfSkylines());
        row.createCell(4).setCellValue(result.getElapsedTime());

        writeToFile();
    }

    private void writeToFile() {
        try (FileOutputStream fileOut = new FileOutputStream(outputFilePath)) {
            workbook.write(fileOut);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
