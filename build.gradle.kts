
plugins {
    application
    id("org.springframework.boot") version ("3.4.1")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

apply(plugin = "io.spring.dependency-management")

the<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension>().apply {
    imports {
        mavenBom("io.github.resilience4j:resilience4j-bom:2.3.0")
    }
}

repositories {
	mavenCentral()
    maven {
        url = uri("https://oss.sonatype.org/content/repositories/snapshots")
    }

}

//configurations.all {
//	resolutionStrategy.cacheChangingModulesFor 0, "seconds"
//}

val resilience4jVersion = "2.3.0"

tasks.withType<Test> {
	useJUnitPlatform()
}

dependencies {

    implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.boot:spring-boot-starter-webflux")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-aop")

    	implementation("io.github.resilience4j:resilience4j-spring-boot3")
	implementation("io.github.resilience4j:resilience4j-all") // Optional, only required when you want to use the Decorators class
	implementation("io.github.resilience4j:resilience4j-reactor")
	implementation("io.micrometer:micrometer-registry-prometheus")

	//implementation("de.codecentric:chaos-monkey-spring-boot:2.6.1")
	
	implementation("io.vavr:vavr-jackson:0.10.3")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
}
