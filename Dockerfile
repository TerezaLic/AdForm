FROM rocker/rstudio-stable:latest
  RUN R -e "install.packages(c('jsonlite', 'lubridate','devtools'), repos='http://cran.rstudio.com/')"
 -RUN R -e "install_github("keboola/r-docker-application",ref = "master")"
 -RUN R -e "install_github("keboola/r-custom-application-example-package")"
  
  CMD ["R"]
  MAINTAINER Tereza/@revolt.bi
