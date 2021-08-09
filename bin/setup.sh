#!/usr/bin/env sh

# Should be run as root.

cd `dirname $0`

if (( $EUID != 0 )); then
  echo "Setting up as non-root user"
  BASE_DIR=$HOME/.iot-data-utility
else
  echo "Setting up as root user"
  BASE_DIR=/usr/local/iot-data-utility
fi

mkdir -p $BASE_DIR/bin
mkdir -p $BASE_DIR/lib

# Cleanup previous installation
rm -f $BASE_DIR/lib/*.jar
rm -f $BASE_DIR/bin/*

cp -f datagencli $BASE_DIR/bin
cp -f datagenmr $BASE_DIR/bin

if [ -f iot-data-utility-shaded.jar ]; then
    cp -f iot-data-utility-shaded.jar $BASE_DIR/lib
fi

chmod -R +r $BASE_DIR
chmod +x $BASE_DIR/bin/datagencli
chmod +x $BASE_DIR/bin/datagenmr

if (( $EUID == 0 )); then
  echo "Setting up global links"
  ln -sf $BASE_DIR/bin/datagencli /usr/local/bin/datagencli
  ln -sf $BASE_DIR/bin/datagenmr /usr/local/bin/datagenmr
else
  mkdir -p $HOME/bin
  ln -sf $BASE_DIR/bin/datagencli $HOME/bin/datagencli
  ln -sf $BASE_DIR/bin/datagenmr $HOME/bin/datagenmr
  echo "Executable in \$HOME/bin .  Add this to the environment path."
fi







