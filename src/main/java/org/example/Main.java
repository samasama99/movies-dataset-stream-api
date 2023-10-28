package org.example;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        try {
            var csvParserBuilder = new CSVParserBuilder();

            Arrays.stream(args)
                    .parallel()
                    .map(file -> {
                        var parser = csvParserBuilder
                                .withSeparator(',')
                                .withQuoteChar('"')
                                .withIgnoreQuotations(false)
                                .build();
                        try (Reader dataset = new FileReader(file)) {
                            var csvReaderBuilder = new CSVReaderBuilder(dataset);
                            var csvReader = csvReaderBuilder.withCSVParser(parser).build();
                            return Pair.of(
                                    file,
                                    csvReader
                                            .readAll()
                                            .stream()
                                            .skip(1)
//                                            .parallel()
                                            .filter(row -> row[6] != null && !row[6].isBlank())
                                            .map(row -> row[6])
                                            .mapToDouble(Double::parseDouble)
                                            .average().orElse(0.)
                            );
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).forEach(System.out::println);
        } catch (Exception e) {
            System.err.println("[Error] " + e.getMessage());
        }
    }
}