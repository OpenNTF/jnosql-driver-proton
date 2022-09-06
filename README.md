# Jakarta NoSQL Driver For Domino Via Proton

This repository contains a Jakarta NoSQL driver that connects to a Domino server via Proton (the AppDev Pack). This driver targets Jakarta EE 9 and assumes that your application includes CDI 3.0 or above and Bean Validation 3.0 or above. This driver builds on the core driver components developed in the [XPages Jakarta EE Support project](https://github.com/OpenNTF/org.openntf.xsp.jakartaee/).

## Implementation Notes

#### Limitations

Because Proton does not expose a number of Domino APIs and concepts, this driver is limited compared to the LSXBE driver. Specifically:

- Sorting is not available when querying
- Views entries are not available
- Entities returned by `@ViewDocuments` may not be in view order
- DominoDocumentCollectionManager#getByNoteId is unavailable
- Folder add/remove methods are not available
- Transaction support is not available

#### Jakarta NoSQL Dependencies

Because official builds of Jakarta NoSQL and the JNoSQL implementation target JEE8 upstream APIs, this project uses [transformed](https://github.com/eclipse/transformer) artifacts hosted on OpenNTF's Maven repository using different coordinates and version numbers from the standard versions.

## Requirements

- Java 8 or above
- The `com.hcl.domino:domino-db` Maven artifact of at least version 1.6.5 installed

If you have downloaded the AppDev Pack ZIP, you can install the `domino-db` driver in your local repository by extracting the "pom.xml" file from its META-INF/maven directory and then running a command like:

```sh
mvn install:install-file -Dfile=domino-db-1.6.5.jar -DpomFile=pom.xml -Djavadoc=domino-db-1.6.5-javadoc.jar
```

## License

The code in the project is licensed under the Apache License 2.0. The dependencies in the binary distribution are licensed under compatible licenses - see NOTICE for details.