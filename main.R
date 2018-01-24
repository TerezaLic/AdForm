
#=======Install the libraries=========#
library(lubridate,quietly = TRUE)
library(httr, quietly = TRUE)
library(jsonlite, quietly = TRUE)
library('keboola.r.docker.application')
library('keboola.sapi.r.client', quietly = TRUE)
library(purrr, quietly = TRUE)
library(foreach, quietly = TRUE)

#=======CONFIGURATION========#

# initialize application
app <- keboola.r.docker.application::DockerApplication$new('/data/')
app$readConfig()


# access the supplied value of 'myParameter'
dateFrom<-app$getParameters()$dateFrom
dateTo<-app$getParameters()$dateTo
grouping<-app$getParameters()$groupBy
ContentType<-"text/csv"
user<-app$getParameters()$user
password<-app$getParameters()$'#password'
#outDestination <- app$getParameters()$bucket


#=======Actual API call========#

# get session token

url <- "https://api.adform.com/"
req <- httr::POST(url,path="v1/dmp/token",body=list(grant_type="password",username=user,password=password),encode = "form")
json <- httr::content(req, as = "text")
returnData<-fromJSON(json)
apiKey <- paste("Bearer ",returnData$access_token)

# catch login errors
if (req$status_code != 200) stop("unauthorized - verify username & password")


# get list of data providers
pIdata<-GET(url,path="/v1/dmp/dataproviders",add_headers(Authorization = apiKey))%>%content("text", encoding = "UTF-8")%>%fromJSON(flatten=TRUE,simplifyDataFrame = TRUE)
pid<-pIdata[["id"]]

# DEFINE FUNCTIONS

# define API function w/o parameters
get_report<-function(endpoint){
    req<- httr::GET(url,path=endpoint,query=list(from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    datasource<-httr::content(req, as="parse")
    fname=basename(endpoint)
    csvFileName<-paste("/data/out/tables/",fname,".csv",sep = "")
    write.csv(datasource,file=csvFileName,row.names = FALSE)
    # write table metadata - set new primary key
    # app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('Date'))
}

# define API function with ProviderId parameter
get_report_pId<-function(pid,endpoint){
  datasource<-foreach(i=pid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(dataProviderId=i,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    datasource<-httr::content(req, as="parse")
  }
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
}

# define API function with ProviderId & Group by parameters
get_report_pIdGb<-function(pid,endpoint){
  datasource<-foreach(i=pid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(groupBy=grouping,dataProviderId=i,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    datasource<-httr::content(req, as="parse")
  }
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,"_",grouping,".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
}

# define API function with ProviderId & Group by parameters
get_report_pIdGb2<-function(pid,endpoint){
  datasource<-foreach(i=pid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(groupBy=grouping,dataProviderId=i,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    datasource<-httr::content(req, as="parse")
    df<-as.data.frame(datasource)
    df$byProviderId<-i
  }
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,"_",grouping,".csv",sep = "")
  write.csv(df,file=csvFileName,row.names = FALSE)
}

# data load
get_report(endpoint="v2/dmp/reports/billing/overall")
get_report_pId(endpoint="/v2/dmp/dataproviders/{dataProviderId}/audience",pid)
get_report_pIdGb(endpoint="v2/dmp/reports/datausage",pid)
get_report_pIdGb2(endpoint="v2/dmp/reports/audience",pid)



