# cs425mp3

## Develop environment

- IntelliJ IDEA
- Mac OS
- OpenJDK 1.8.181 x64
- Maven 3 with wrapper

## Build 

Build system is **Maven 3** with **maven-surefire-plugin**.

You need to equip and configure environment:

- JDK version 1.8 or above

### To build a runnable JAR

```bash
./mvnw package
```

### How to run

1. Open a terminal

2. Build a runnable JAR (See before)

3. Deploy built JAR to all EE-VMs, using the script (You need a private key): `scripts/distribute_jar_to_all.sh`

4. Run the code:
```bash
java -jar mp3-1.0-jar-with-dependencies.jar
```

## Scripts

- To deploy built JAR to all EE-VMs, run: `scripts/distribute_jar_to_all.sh`


