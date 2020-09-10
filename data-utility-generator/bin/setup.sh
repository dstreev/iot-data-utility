#!/usr/bin/env sh

# Should be run as root.

cd `dirname $0`

mkdir -p /usr/local/datagen/bin
mkdir -p /usr/local/datagen/lib

cp -f datagen /usr/local/datagen/bin

# Cleanup previous installation
rm -f /usr/local/datagen/lib/*.jar

if [ -f ../target/data-utility-generator-shaded.jar ]; then
    cp -f ../target/data-utility-generator-shaded.jar /usr/local/datagen/lib
fi

if [ -f data-utility-generator-shaded.jar ]; then
    cp -f data-utility-generator-shaded.jar /usr/local/datagen/lib
fi

chmod -R +r /usr/local/datagen
chmod +x /usr/local/datagen/bin/datagen

ln -sf /usr/local/datagen/bin/datagen /usr/local/bin/datagen


