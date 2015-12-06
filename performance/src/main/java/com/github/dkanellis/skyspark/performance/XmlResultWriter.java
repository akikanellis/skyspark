package com.github.dkanellis.skyspark.performance;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.github.dkanellis.skyspark.api.algorithms.Preconditions.checkNotEmpty;

public class XmlResultWriter implements ResultWriter {

    private final String outputFilePath;
    private final Workbook workbook;
    private final Sheet resultSheet;

    public XmlResultWriter(String outputFilePath) {
        this.outputFilePath = checkNotEmpty(outputFilePath);
        workbook = new HSSFWorkbook();
        resultSheet = workbook.createSheet("Results");
    }

    @Override
    public void writeResult(String algorithmName, final long elapsedMillis, String dataType, final int dataSize) {
        Row row = resultSheet.createRow(resultSheet.getLastRowNum() + 1);
        row.createCell(0).setCellValue(algorithmName);
        row.createCell(1).setCellValue(dataType);
        row.createCell(2).setCellValue(dataSize);
        row.createCell(3).setCellValue(elapsedMillis);

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
