plugins {
    application
    id("com.diffplug.spotless") version "5.12.1"
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

sourceSets {
    main {
        java {
            setSrcDirs(listOf("src"))
        }
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("commons-cli:commons-cli:1.4")
    implementation("org.slf4j:slf4j-log4j12:1.7.30")
    implementation("org.unbescape:unbescape:1.1.6.RELEASE")
    implementation("org.wikidata.wdtk:wdtk-datamodel:0.11.1")
    implementation("org.wikidata.wdtk:wdtk-dumpfiles:0.11.1")
    implementation("org.wikidata.wdtk:wdtk-rdf:0.11.1")
    implementation("org.wikidata.wdtk:wdtk-wikibaseapi:0.11.1")
}

application {
    mainClass.set("wikidatadumpprocessor.WikidataDumpProcessor")
}

spotless {
    format("misc") {
        target("*.gitignore")
        trimTrailingWhitespace()
        indentWithSpaces(4)
        endWithNewline()
    }
    java {
        googleJavaFormat().aosp()
        licenseHeaderFile(file("${project.rootDir}/LICENSE.header"))
    }
    kotlinGradle {
        target("*.gradle.kts")
        ktlint()
    }
}
