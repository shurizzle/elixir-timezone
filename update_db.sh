#!/bin/sh

# setup your local environment
rm -rf priv/tzdata
mkdir -p priv/tzdata

# download the timezone data files
wget 'ftp://ftp.iana.org/tz/tzdata-latest.tar.gz'

# extract files
tar -xvzf tzdata-latest.tar.gz -C priv/tzdata

# remove useless files
rm tzdata-latest.tar.gz
cd priv/tzdata
rm -f *.sh *.tab factory Makefile
