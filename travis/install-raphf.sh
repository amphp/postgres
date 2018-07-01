#!/usr/bin/env bash

git clone https://github.com/m6w6/ext-raphf;
pushd ext-raphf;
phpize;
./configure;
make;
make install;
popd;
echo "extension=raphf.so" >> "$(php -r 'echo php_ini_loaded_file();')";
rm -rf ext-raphf
