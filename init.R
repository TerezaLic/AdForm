library('devtools')

# install the transformation application ancestors
devtools::install_version("curl", version = "4.1", repos = "http://cran.us.r-project.org",quiet = TRUE)
devtools::install_github('keboola/r-application', ref = "master", force = TRUE)
devtools::install_github('keboola/r-docker-application', ref = "master", force = TRUE)
devtools::install_github("keboola/sapi-r-client", ref = "master", force = TRUE)
devtools::install_github("cloudyr/aws.signature")
