#!/bin/bash

# download the data
wget http://homepages.cwi.nl/~boncz/job/imdb.tgz
mv imdb.tgz imdb/
tar zxvf imdb/imdb.tgz -C imdb
rm imdb/imdb.tgz
