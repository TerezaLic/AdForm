#Install the libraries
library(jsonlite)
library(lubridate)

+#=======CONFIGURATION========#

library(keboola.r.docker.application)
# initialize application
app <- DockerApplication$new('/data/')
app$readConfig()
# access the supplied value of 'myParameter'
apiKey<-app$getParameters()$`#apiKey`

##Catch config errors

if(is.null(apiKey)) stop("enter your apiKey in the user config field")

#=======Actual API call========#
# report Datausage je automaticky group by Agency
url <- "https://api.adform.com/v2/dmp/reports/datausage"
req <- httr::GET(url, httr::add_headers(Authorization = ""))
json <- httr::content(req, as = "text")
Datausage<-fromJSON(json)
write.csv(Datausage,"out/tables/Datausage.csv")

# report Audience vyžaduje Provider Id, Json2 file problem format
url2 <- "https://api.adform.com/v2/dmp/reports/audience?dataProviderId=11392"
req2 <- httr::GET(url2, httr::add_headers(Authorization = ""))
json2 <- httr::content(req2, as = "text")
Audience<-fromJSON(json2)
write.csv(Audience,"out/tables/Audience.csv")

# report Audience2
url3 <- "https://api.adform.com/v2/dmp/dataproviders/11392/audience"
req3 <- httr::GET(url3, httr::add_headers(Authorization = ""))
json3 <- httr::content(req3, as = "text")
DataProvider_audience<-fromJSON(json3)
write.csv(DataProvider_audience,"out/tables/DataProvider_audience.csv")

# report billing
url4 <- "https://api.adform.com/v2/dmp/reports/billing/overall"
req4 <- httr::GET(url4, httr::add_headers(Authorization = ""))
json4 <- httr::content(req4, as = "text")
Billing<-fromJSON(json4)
# data cleaning
Billing$date<-ymd_hms(content4$date)
write.csv(Billing,"out/tables/Billing.csv")
