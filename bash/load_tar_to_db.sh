#!/usr/bin/env bash

tar=tarfile.tar.gz

DATABASE_URL="fiche_inspe_4015:IdwOESjNz1nmX5NkeGIY@locahost:10000/fiche_inspe_4015"

pg_restore --clean --if-exists --no-owner --no-privileges  --dbname $DATABASE_URL "tarfile/20211122000001_trackdechet_8889.pgsql"

