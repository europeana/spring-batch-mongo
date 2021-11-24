# spring-batch-mongo

This library provides MongoDB JobRepository support for Spring Batch.

### Installation

- Ensure that you have the Europeana Maven repository configured. Add the following to your `pom.xml` file for this:
    ```
      <repositories>
        <repository>
          <id>libs-release</id>
          <name>europeana-releases</name>
          <url>https://artifactory.eanadev.org/artifactory/libs-release</url>
        </repository>
        <repository>
          <id>libs-snapshots</id>
          <name>europeana-snapshots</name>
          <url>https://artifactory.eanadev.org/artifactory/libs-snapshot</url>
        </repository>
      </repositories>
    ```

- Then add the following Maven dependency for the spring-batch-mongo library

    ```
    <dependency>
        <groupId>eu.europeana.api</groupId>
        <artifactId>spring-batch-mongo</artifactId>
        <version>1.0.3</version>
    </dependency>
    ```

### Usage
 - Create a Morphia Datastore for connecting to Mongo

    ```
    public Datastore batchDatastore() {
        Datastore datastore = Morphia.createDatastore(
                MongoClientSettings.builder()
             .applyConnectionString("mongodb://<user>:<password>@<host>:<port>")
                .build()
                ), "<databaseName>");
            
     // Required to create indices on database
        datastore.getMapper().mapPackage(PackageMapper.class.getPackageName());
        datastore.ensureIndexes();
        return datastore;
    }
    ```

    replacing `<user>`, `<password>`, `<host>`, `<port>` and `<databaseName>` with the correct values.


* Configure a `MongoBatchConfigurer` bean, using the Morphia Datastore and a TaskExecutor. 

    ```
     @Bean
     public MongoBatchConfigurer mongoBatchConfigurer() {
       return new MongoBatchConfigurer(batchDatastore(), new SimpleAsyncTaskExecutor());
     }
    ```
    _Any implementation of the Spring `TaskExecutor` interface can be used to control how jobs are asynchronously executed._





