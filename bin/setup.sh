#!/usr/bin/env sh

# Should be run as root.

cd `dirname $0`

mkdir -p /usr/local/datagen/bin
mkdir -p /usr/local/datagen/lib

cp -f datagencli /usr/local/datagen/bin
cp -f datagenmr /usr/local/datagen/bin

# Cleanup previous installation
rm -f /usr/local/datagen/lib/*.jar

if [ -f ../target/iot-data-utility-shaded.jar ]; then
    cp -f ../target/iot-data-utility-shaded.jar /usr/local/datagen/lib
fi

if [ -f iot-data-utility-shaded.jar ]; then
    cp -f iot-data-utility-shaded.jar /usr/local/datagen/lib
fi

chmod -R +r /usr/local/datagen
chmod +x /usr/local/datagen/bin/datagencli
chmod +x /usr/local/datagen/bin/datagenmr

ln -sf /usr/local/datagen/bin/datagencli /usr/local/bin/datagencli
ln -sf /usr/local/datagen/bin/datagenmr /usr/local/bin/datagenmr


