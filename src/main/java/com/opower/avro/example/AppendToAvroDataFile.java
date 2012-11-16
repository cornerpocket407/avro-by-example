package com.opower.avro.example;

import java.util.Random;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.opower.avro.example.model.Person;

/**
 * Shows how to append to an Avro data file.  Keep in mind that when you append
 * to a data file, you must use the same writer schema that was used to
 * originally create it, since the schema is stored once in the header and
 * reread when you reopen the file for appending.
 */
public class AppendToAvroDataFile {
    public static void usage() {
        System.err.printf(
                "usage: %s <file path to write to> [<# of data to append>]%n",
                AppendToAvroDataFile.class.getName()
                );
    }

    public static void main(String[] args) throws Exception {
        int dataCount = 1;
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
        // LocalFileSystem does not support append, so force use of
        // RawLocalFileSystem, whch does support append.
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");

        FileSystem fs = filePath.getFileSystem(config);
        DatumWriter<Person> datumWriter = new SpecificDatumWriter<Person>(Person.class);
        DataFileWriter<Person> fileWriter = new DataFileWriter<Person>(datumWriter);

        if (fs.createNewFile(filePath)) {
            System.err.println("Created new file " + filePath);
            fileWriter.create(Person.SCHEMA$, fs.append(filePath));
        }
        else {
            System.err.println("File already existed " + filePath);
            fileWriter.appendTo(new FsInput(filePath, config), fs.append(filePath));
        }

        Random random = new Random();

        for (int i = 0; i < dataCount; i++) {
            Person person = Person.newBuilder()
                .setUsername("User" + random.nextInt(20))
                .setPassword("fakepassword")
                .setJoinedOn(System.currentTimeMillis())
                .build();
            System.err.println("Person: " + person);
            fileWriter.append(person);
        }
        fileWriter.flush();
        fileWriter.close();
    }
}
