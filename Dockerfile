
FROM rocker/rstudio-stable:latest

RUN R -e "install.packages(c('jsonlite', 'lubridate','devtools'), repos='http://cran.rstudio.com/')"


# Run the application
ENTRYPOINT Rscript /home/main.R /data/
