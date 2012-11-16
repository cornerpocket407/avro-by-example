package com.opower.avro.example;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.opower.avro.example.model.Person;

/**
 * An example of reading some data from an Avro data file.
 */
public class ReadFromAvroDataFile {
    public static void usage() {
        System.err.printf(
                "usage: %s <file path to read from> [<# of data to read>]%n",
                ReadFromAvroDataFile.class.getName()
                );
    }

    public static void main(String[] args) throws Exception {
        int dataCount = Integer.MAX_VALUE;
        if (args.length < 1) {
            usage();
            System.exit(1);
        }
        Path filePath = new Path(args[0]);
        if (args.length > 1) {
            try {
                dataCount = Integer.parseInt(args[1]);
            }
            catch (NumberFormatException nfe) {
                usage();
                System.exit(2);
            }
        }

        Configuration config = new Configuration();

        FileSystem fs = filePath.getFileSystem(config);
        if (!fs.exists(filePath)) {
            System.err.println("No file to read at: " + filePath);
            System.exit(3);
        }

        DatumReader<Person> datumReader = new SpecificDatumReader<Person>(Person.class);
        FileReader<Person> fileReader =
            DataFileReader.<Person>openReader(new FsInput(filePath, config), datumReader);

        Person person = new Person();
        int count = 0;
        while (++count <= dataCount && fileReader.hasNext()) {
            person = fileReader.next(person);
            System.out.printf("%3d Person: %s%n", count, person);
        }
        fileReader.close();
    }
}
