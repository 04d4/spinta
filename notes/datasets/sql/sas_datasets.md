# Instruction how to install and run the project with SAS Datasets backend

## Installing requirements

### Python

Install Python 3.10 or higher according to your OS.

```zsh
    sudo apt update
    sudo apt install python3-pip python3-venv
```

### Java

Install Java 9-11 according to your OS.

```zsh
    sudo apt install openjdk-11-jre-headless
```

### SAS Drivers for JDBC

Download SAS Drivers for JDBC from [SAS Support](https://documentation.sas.com/doc/lt/jdbcref/9.4/p0kmu0fvvmci5qn1jn6xjlnmwi90.htm).

```zsh
    cd Downloads
    tar xvf jdbcdrivers__94300__lax__xx__web__1.tar -C ~/jdbc
    mkdir -p ~/sas
    find ~/jdbc/products/jdbcdrivers__94300__prt__xx__sp0__1/eclipse/plugins  -name "*.jar" -exec cp {} ~/sas/ \;
    # cleanup; optional
    rm jdbcdrivers__94300__lax__xx__web__1.tar
    rm -r ~/jdbc
```

Set `CLASSPATH` environment variable to the path of the SAS Drivers for JDBC.

```zsh
    export CP='/home/ubuntu/sas'
    export CLASSPATH=$CP/glassfish-corba-internal-api.jar:$CP/glassfish-corba-omgapi.jar:$CP/glassfish-corba-orb.jar$CP/sas.core.jar:$CP/sas.svc.connection.jar:$CP/sas.security.sspi.jar:$CP/log4j-api.jar:$CLASSPATH:.
    echo $CLASSPATH
```

Better set this variable in your `.bashrc` or `.zshrc` file.

### Python connection to JDBC
