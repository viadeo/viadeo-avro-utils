import AssemblyKeys._ // put this at the top of the file

assemblySettings

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter { c =>
    List(
      "netty-3.2.4.Final.jar",
      "hadoop-common-2.0.0-cdh4.1.2.jar",
      "mockito-all-1.8.5.jar",
      "servlet-api-2.5.jar",
      "servlet-api-2.5-6.1.14.jar",
      "servlet-api-2.5-20081211.jar",
      "jsp-2.1-6.1.14.jar",
      "jsp-api-2.1-6.1.14.jar",
      "objenesis-1.2.jar",
      "asm-commons-4.0.jar",
      "jruby-complete-1.6.5.jar",
      "jetty-util-6.1.26.jar",
      "jetty-6.1.26.jar",
      "velocity-1.7.jar",
      "commons-beanutils-1.7.0.jar",
      "stax-api-1.0.1.jar",
      "commons-collections-3.2.1.jar",
      "asm-3.2.jar",
      "minlog-1.2.jar",
      "scala-stm_2.10.0-0.6.jar",
      "play-json_2.10-2.2-SNAPSHOT.jar",
      "slf4j-log4j12-1.6.6.jar",
      "stax-api-1.0-2.jar",
      "jcl-over-slf4j-1.7.5.jar",
      "play_2.10-2.2.0.jar",
      "asm-3.1.jar",
      "avro-ipc-1.7.4-tests.jar"
    ).contains(c.data.getName)
  }
}

test in assembly := {}

//mainClass in assembly := Some("com.example.Main")
