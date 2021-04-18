plugins {
    application
    id("com.diffplug.spotless") version "5.12.1"
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.wikidata.wdtk:wdtk-datamodel:0.11.1")
    implementation("org.wikidata.wdtk:wdtk-dumpfiles:0.11.1")
    implementation("org.wikidata.wdtk:wdtk-rdf:0.11.1")
    implementation("org.wikidata.wdtk:wdtk-wikibaseapi:0.11.1")
    implementation("org.slf4j:slf4j-log4j12:1.7.30")
    implementation("org.unbescape:unbescape:1.1.6.RELEASE")
}

application {
    mainClass.set("com.lschmelzeisen.kgevolve.KgEvolve")
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
