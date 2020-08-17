#!/usr/bin/env sh

# Should be run as root.

cd `dirname $0`

mkdir -p /usr/local/datagen/bin
mkdir -p /usr/local/datagen/lib

cp -f datagen /usr/local/datagen/bin

# Cleanup previous installation
rm -f /usr/local/datagen/lib/*.jar

if [ -f ../target/datagen-full-bin.jar ]; then
    cp -f ../target/datagen-full-bin.jar /usr/local/datagen/lib
fi

if [ -f datagen-full-bin.jar ]; then
    cp -f datagen-full-bin.jar /usr/local/datagen/lib
fi

chmod -R +r /usr/local/datagen
chmod +x /usr/local/datagen/bin/datagen

ln -sf /usr/local/datagen/bin/datagen /usr/local/bin/datagen


