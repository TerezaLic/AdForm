#!/bin/bash
set -e

R CMD build 
R CMD check --as-cran
