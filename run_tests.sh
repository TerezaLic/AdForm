#!/bin/bash
set -e

R CMD build 
R CMD check keboola.r.transformation_1.0.tar.gz --as-cran --no-manual
