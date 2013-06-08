#!/bin/bash
cd dist
cp seedrocket.hadoop.jar seedrocket.hadoop.jar.bck
jar uf seedrocket.hadoop.jar lib/*
cd ..
rm -r release
mkdir release
cp dist/seedrocket.hadoop.jar release/.
mv dist/seedrocket.hadoop.jar.bck dist/seedrocket.hadoop.jar
echo "Packed jar should be in folder 'release'"
echo "Please type 'hadoop jar release/seedrocket.hadoop.jar' to proceed";
