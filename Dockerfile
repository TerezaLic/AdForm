FROM rocker/rstudio-stable:latest
RUN R -e "install.packages(c('jsonlite', 'lubridate'), repos='http://cran.rstudio.com/')"
CMD ["R"]
MAINTAINER Tereza/@revolt.bi
