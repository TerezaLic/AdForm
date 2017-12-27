FROM rocker/rstudio-stable:latest
COPY . /code/
ENTRYPOINT Rscript ./main.R 
