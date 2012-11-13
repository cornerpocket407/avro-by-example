package com.opower.avro.example;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import java.io.IOException;

/**
 * Demonstrates how changing the name of a schema will change the schema's
 * fingerprint.  This means that you cannot keep multiple versions of your
 * schema in your path to have classes generated from them, since you must keep
 * your schema fingerprints consistent in order to publish and identify them
 * properly.
 */
public class ChangingNameChangesFingerprint {
    public static void main(String[] args) throws Exception {
        Class clazz = ChangingNameChangesFingerprint.class;
        Schema original = new Schema.Parser().parse(clazz.getResourceAsStream("/order-orig.avsc"));
        Schema renamed = new Schema.Parser().parse(clazz.getResourceAsStream("/order-renamed.avsc"));

        long originalFingerprint = SchemaNormalization.parsingFingerprint64(original);
        long renamedFingerprint = SchemaNormalization.parsingFingerprint64(renamed);

        System.out.printf("Original: %24d%nRenamed:  %24d%n", originalFingerprint, renamedFingerprint);
    }
}
