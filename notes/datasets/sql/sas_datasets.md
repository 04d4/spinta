# Instruction how to install and run the project with SAS Datasets backend

## Installing requirements

### Add user 'spinta'

```zsh
    sudo adduser --disabled-password --home /home/spinta --gecos "" spinta
    sudo su - spinta
    sudo usermod -aG sudo spinta
    sudo userdel --force --remove spinta
```

### Python

Install Python 3.10 or higher according to your OS.

```zsh
    sudo apt update
    sudo apt install -y python3-pip python3-venv
```

### Java

Install Java 9-11 according to your OS.

```zsh
    sudo apt install -y openjdk-11-jre-headless
```

### SAS Drivers for JDBC

Download SAS Drivers for JDBC from [SAS Support](https://documentation.sas.com/doc/lt/jdbcref/9.4/p0kmu0fvvmci5qn1jn6xjlnmwi90.htm).

```zsh
    mkdir /tmp/jdbc
    tar xvf ~/Downloads/jdbcdrivers__94300__lax__xx__web__1.tar -C /tmp/jdbc
    unzip /tmp/jdbc/products/jdbcdrivers__94300__prt__xx__sp0__1/jdbcdrivers_vjr.zip -d /tmp/jdbc/drivers
    sudo mkdir -p /opt/jdbc
    sudo chown -R spinta:spinta /opt/jdbc
    sudo chown -R oa:oa /opt/jdbc
    find /tmp/jdbc/drivers/eclipse/plugins  -name "*.jar" -exec cp {} /opt/jdbc \;
    ls -al /opt/jdbc
    # cleanup
    rm -r /tmp/jdbc
```

Set `CLASSPATH` environment variable to the path of the SAS Drivers for JDBC.

```zsh
    export CP='/opt/jdbc'
    # log4j
    export X1=$CP/log4j-core.jar
    # Corba support for Java 11
    export X2=$CP/glassfish-corba-internal-api.jar:$CP/glassfish-corba-omgapi.jar:$CP/glassfish-corba-orb.jar
    export X3=$CP/pfl-basic.jar:$CP/pfl-tf.jar:
    # SAS Drivers for JDBC
    export X4=$CP/sas.core.jar:$CP/sas.svc.connection.jar:$CP/sas.security.sspi.jar:$CP/log4j-api.jar
    # full CLASSPATH
    export CLASSPATH=$X4:$X3:$X2:$X1:$CLASSPTH
    echo $CLASSPATH
```

Other solution to update CLASSPATH:

```zsh
    export CLASSPATH=$(find ~/sas -name "*.jar" | tr '\n' ':' | sed 's/:$//')
```

**Note:** The `CLASSPATH` environment variable is set only for the current session.

Better set this variable in your `.bashrc` or `.zshrc` file.

### Poetry

```zsh
    sudo apt install -y curl git
    curl -sSL https://install.python-poetry.org | python3 -
    export PATH="$HOME/.local/bin:$PATH"
    python3 -m venv .venv
    source .venv/bin/activate
    # option 1
    git clone https://github.com/atviriduomenys/spinta
    cd spinta
    poetry install
    git switch 1460-sas-db
    # option 2
    pip install --pre spinta
```

### Python connection to JDBC

```zsh
    source .venv/bin/activate
    pip install jaydebeapi
```

### SAS username and password

```zsh
# load username and password from .env
set -a; source .env; set +a
# url encode
export SPINTA_SAS_USER=$(printf %s $SAS_USER | jq -sRr @uri)
export SPINTA_SAS_PASSWORD=$(printf %s $SAS_PASSWORD | jq -sRr @uri)
# verify
echo "$SAS_USER $SAS_PASSWORD"
echo "$SPINTA_SAS_USER $SPINTA_SAS_PASSWORD"
```

## Running the project

```zsh
    source .venv/bin/activate
    pyenv virtualenv 3.11 sp311c
    pyenv activate sp311c
    poetry run spinta inspect -r sql "sas+jdbc://$SPINTA_SAS_USER:$SPINTA_SAS_PASSWORD@192.168.122.27:8597/?schema=IMEK" -o dist/manifest_IMEK_9.csv
    poetry run spinta inpsect
```
