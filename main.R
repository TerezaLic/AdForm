
#=======Install the libraries=========#
 require(devtools)
 #curl new versions not working libcurl 7.52
 install_version("curl", version = "4.1", repos = "http://cran.us.r-project.org",quiet = TRUE)


library(curl,warn.conflicts=FALSE, quietly=TRUE)
library(plyr)

library(lubridate, warn.conflicts=FALSE, quietly=TRUE)
library(httr, warn.conflicts=FALSE, quietly=TRUE)
library(jsonlite, warn.conflicts=FALSE, quietly=TRUE)
library('keboola.r.docker.application', warn.conflicts=FALSE, quietly=TRUE)
#library('keboola.sapi.r.client', quietly = TRUE)
library(purrr, warn.conflicts=FALSE, quietly=TRUE)
library(foreach, warn.conflicts=FALSE, quietly=TRUE)
library(stringr, warn.conflicts = FALSE, quietly = TRUE)
library(dplyr, warn.conflicts = FALSE, quietly = TRUE)


#=======CONFIGURATION========#

# initialize application
app <- keboola.r.docker.application::DockerApplication$new('/data/')
app$readConfig()

# access the supplied value of 'myParameter'
days_past<-app$getParameters()$from
user<-app$getParameters()$user
password<-app$getParameters()$'#password'
# default values for filters = (All), considered below as "select_all" / no filtering
id<-app$getParameters()$filtry$id
textstr<-app$getParameters()$filtry$text
#3 options for filter: "category", "data consumers", "segment/audience"
filterUI<-app$getParameters()$filtry$filter

ContentType<-"text/csv"
#outDestination <- app$getParameters()$bucke

## Create the date from where we take data
days_past<-ifelse(is.null(days_past),1,as.numeric(days_past))
from<-Sys.Date()-days_past

#=======Actual API call========#

# get session token

url <- "https://api.adform.com/"
req <- httr::POST(url,path="v1/dmp/token",body=list(grant_type="password",username=user,password=password),encode = "form")
json <- httr::content(req, as = "text")
returnData<-fromJSON(json)
apiKey <- paste("Bearer ",returnData$access_token)

# catch login errors
if (req$status_code != 200) stop("unauthorized - verify username & password")


#=======DEFINE FUNCTIONS========#

# funciton to clean nested Json / split list values into more columns
flatten<-function (l) {
  if (!is.list(l)) return(l)
  do.call('rbind', lapply(l, function(x) `length<-`(x, 2)))
}
# ?? různá délka outputu problém pro KBC, zatím poznámka do user dokumentace

# get list of Ids
get_Id_list<-function(file){
  read.csv(file,sep = ",", quote="\"")[ ,1]
}                          

# get list of Ids & Names (needed for UI filtering)
get_SID_list<-function(file){
  read.csv(file,sep = ",", quote="\"")[ ,c(1, 8)]
}            

# define API function w/o parameters 
get_report<-function(endpoint){
  req<- httr::GET(url,path=endpoint,httr::add_headers(Accept = ContentType,Authorization = apiKey))
  datasource<-httr::content(req, as="parse")
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
  if (fname=="overall") {
    # write table metadata - set new primary key
    app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('DataProviderId'), incremental=TRUE)}
  else {
    app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('Id'),incremental=TRUE)
  }
}

# define API function with ProviderId parameter
get_report_pId<-function(pid,endpoint,filterType){
  datasource<-foreach(i=pid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(dataProviderId=i) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    # if/else to implement UI filters (teststr, Id, filterUI)
    if(filterType==filterUI & !(id=="(All)") & textstr=="(All)"){
      datasource<-httr::content(req, as="parse")%>%filter(Id == id)
    }  else if (filterType==filterUI & id=="(All)" & !(textstr=="(All)")){
      datasource<-httr::content(req, as="parse")%>%filter(str_detect(Name, fixed(textstr,ignore_case=TRUE)))
    } else if(filterType==filterUI & !(id=="(All)") & !(textstr=="(All)")){
      datasource<-httr::content(req, as="parse")%>%filter(str_detect(Name, fixed(textstr,ignore_case=TRUE)))%>%filter(Id == id)
    } else
      datasource<-httr::content(req, as="parse")
  }
  names(datasource)<- gsub(" ", "_", names(datasource))
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,"_by_PID",".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
  # write table metadata - set new primary key
  if(fname=="segments"){
    app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('Audience_ID'),incremental=TRUE)}
  else {
    app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('Id'),incremental=TRUE)
  }
}


# define API function for report Audience
get_report_audience<-function(pid,endpoint){
  datasource<-foreach(i=pid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(groupBy="date",dataProviderId=i,from=from) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    datasource<-httr::content(req, as="parse")
    names(datasource)<- gsub(" ", "_", names(datasource))
    df<-as.data.frame(datasource)
    # if/else to implement UI filters (teststr, Id, filterUI)
    
    if (filterUI=="category" & length(cid)==0){
      #Writing a message to the console
      write(paste0("No output for selected category. Report",endpoint," is empty."), stdout())
      df<-df[FALSE,]
    } else if (filterUI=="category" & length(cid>=1)){  
      df<-subset(df, Audience_ID %in% sid)
    } else if (filterUI=="segment/audience" & length(sid)==0){
      #Writing a message to the console
      write(paste0("No output for selected segment / audience. Report",endpoint," is empty."), stdout())
      df<-df[FALSE,]  
    } else if (filterUI=="segment/audience" & length(sid)>=1){
      df<-subset(df, Audience_ID %in% sid)
    } else {df}
    
    if (nrow(df)>0){
      df$byProviderId<-i 
    } else {
      df$byProviderId <- character(0) }
  }
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,"_by_date.csv",sep = "")
  write.csv(df,file=csvFileName,row.names = FALSE)
  # write table metadata - set new primary key
  app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('Date','Audience_ID'),incremental=TRUE)
}

# define API function with category ID parameter
get_report_cId<-function(cid,endpoint){
  datasource<-foreach(i=cid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(categoryId=i) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    datasource<-httr::content(req, as="parse")
  }
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,"_by_CID",".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
  # write table metadata - set new primary key
  app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('Audience_ID'),incremental=TRUE)
}



get_report_dcId<-function(dcid,endpoint){
  datasource<-foreach(i=dcid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(dataConsumerId=i) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
    datasource<-httr::content(req, as="parse")
  }
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,"_by_DCID",".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
  # write table metadata - set new primary key
  app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('Id'),incremental=TRUE)
}

# define API function with segment ID parameter / JSON
get_report_SId<-function(sid,endpoint){
  datasource<-foreach(i=sid,.combine='rbind',.multicombine = TRUE)%dopar%{
    req<-httr::GET(url,path=endpoint,query=list(segmentId=i) ,httr::add_headers(Accept = 'application/json',Authorization = apiKey))
    datasource<-httr::content(req, as="text", encoding = "UTF-8")%>%fromJSON(flatten=TRUE,simplifyDataFrame = TRUE)
  }  
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,"_by_SID",".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
  # write table metadata - set new primary key
  if (fname=="comparison") {
    app$writeTableManifest(csvFileName,destination='')
  } else {app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('segmentId', 'date'),incremental=TRUE)}
}

# define function for main report: Datausage                          
get_report_datausage<-function(endpoint){
  req<- httr::GET(url,path=endpoint,query=list(groupBy="none",from=from) ,httr::add_headers(Accept = 'application/json',Authorization = apiKey))
  datasource<-httr::content(req, as="text", encoding = "UTF-8")%>%fromJSON(flatten=TRUE,simplifyDataFrame = TRUE)
  datasource$unifiedTaxonomyIds=NULL
  datasource$unifiedTaxonomies=NULL
  datasource_l<-lapply(datasource, flatten)
  datasource<-do.call(data.frame,datasource_l)
  rm(datasource_l)
  
   # 23.8.2018 addition to ensure that all duplicated rows are included and numeric values sumed
  datasource$key<-paste0(datasource$date,datasource$lineItemId,datasource$orderId,datasource$segmentsGroupId,datasource$segmentIds.1,datasource$segmentIds.2,datasource$segmentIds.3)
  datasource$impressions<-as.numeric(datasource$impressions)
  datasource[sapply(datasource, is.integer)]<-lapply(datasource[sapply(datasource, is.integer)], as.factor)
  # subset<-datasource%>%select(1:22,24,40:46)
  subset<-select(datasource,-impressions,-revenue,-revenueInCampaignCurrency,-revenueInAdvertiserCurrency,-revenueInPartnerPlatformCurrency,-revenueInEuro,-dataProviderRevenue,-dataProviderRevenueInCampaignCurrency,-dataProviderRevenueInAdvertiserCurrency,-dataProviderRevenueInPartnerPlatformCurrency,-dataProviderRevenueInEuro,-adformRevenue,-adformRevenueInCampaignCurrency,-adformRevenueInAdvertiserCurrency
,-adformRevenueInPartnerPlatformCurrency,-adformRevenueInEuro)
  datasource2<-ddply(datasource,.(key),numcolwise(sum))
  datasource2<-datasource2%>%left_join(subset, by=c("key"="key"))
  datasource2$key<-NULL
  datasource<-datasource2
  
  # Unknown number of Segment Ids columns added by "flatten" function - nested Json cleaning.
  # Define them in SegmentCols and use to filter UI segment Id
  SegmentCols<-(datasource[,grepl("^segmentIds",colnames(datasource))])
  if (filterUI=="category" & length(sid)==0){
    datasource<-datasource[FALSE,]
  }
  else if (filterUI=="category" & length(sid)>=1){
    datasource<-datasource[apply(SegmentCols [,],1,function(x) any(x %in% sid)),]
  }
  else if (filterUI=="segment/audience"& length(sid)==0){
    datasource<-datasource[FALSE,]
  }
  else if (filterUI=="segment/audience" & length(sid)>=1){
    datasource<-datasource[apply(SegmentCols [,],1,function(x) any(x %in% sid)),]
  }
  else {datasource}
  
  fname=basename(endpoint)
  csvFileName<-paste("/data/out/tables/",fname,".csv",sep = "")
  write.csv(datasource,file=csvFileName,row.names = FALSE)
   # write table metadata - set new primary key
  app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('date','dataProviderId','sourceId','agencyId','advertiserId','campaignId','orderId','lineItemId','segmentsGroupId','segmentIds_1','segmentIds_2'),incremental=TRUE)
}


# get segment groups not used 20018-07-11.  Output changed by Adform.
get_segmentGr_names<-function(endpoint){
  req<- httr::GET(url,path=endpoint,query=list(groupBy="segment",from=from) ,httr::add_headers(Accept = 'application/json',Authorization = apiKey))
  datasource<-httr::content(req, as="text", encoding = "UTF-8")%>%fromJSON(flatten=TRUE,simplifyDataFrame = TRUE)
  df<-datasource[ ,c(1, 2)]
  fname="SegmentGroups"
  csvFileName<-paste("/data/out/tables/",fname,".csv",sep = "")
  write.csv(df,file=csvFileName,row.names = FALSE)
  # write table metadata - set new primary key
  app$writeTableManifest(csvFileName,destination='' ,primaryKey =c('segmentsGroupId'),incremental=TRUE)
}                                 
#======================DATA LOAD================================================#

# 1. data used as input/filter for other reports

## get list of dataProviders Is  
sink("msg")
suppressMessages(get_report(endpoint="/v1/dmp/dataproviders"))
pid<-get_Id_list("/data/out/tables/dataproviders.csv")                                  

suppressMessages(get_report(endpoint="/v1/dmp/agencies"))

## use dataProvidersId as selection
suppressMessages(get_report_pId(endpoint="/v1/dmp/dataproviders/{dataProviderId}/advertisers",pid,filterType="0"))


suppressMessages(get_report_pId(endpoint="/v1/dmp/dataproviders/{dataProviderId}/categories",pid, filterType="category"))
suppressMessages(get_report_pId(endpoint="/v1/dmp/dataProviders/{dataProviderId}/dataconsumers",pid, filterType="data consumers"))
suppressMessages(get_report_pId(endpoint="/v1/dmp/dataproviders/{dataProviderId}/segments",pid,filterType="0"))


# Get list of  Category_Id a Data Consumer Id according to User filters and use them in other functions- cycling.
cid<-get_Id_list( "/data/out/tables/categories_by_PID.csv")
dcid<-get_Id_list( "/data/out/tables/dataconsumers_by_PID.csv")


# 2. Overall reports - to be filtred based on user selection

## get supplementary tables
if (length(cid)==0){
  #Writing a message to the console
  write(paste0("No output for selected category. Report segments by CID is empty."), stdout())
} else {
  suppressMessages(get_report_cId(endpoint="/v1/dmp/categories/{categoryId}/segments",cid)) 
}

if (filterUI=="category" & length(cid)==0) {
  sid<-NULL
} else if (filterUI=="category" & (count.fields("/data/out/tables/segments_by_CID.csv")>1)){
  sid<-get_Id_list("/data/out/tables/segments_by_CID.csv")
} else if (filterUI=="segment/audience" & !(id=="(All)")){
  sid<-get_SID_list("/data/out/tables/segments_by_PID.csv")%>%filter(str_detect(Audience_ID, fixed(id,ignore_case=TRUE)))%>%select(1)
  sid<-as.numeric(as.character(sid$Audience_ID))
} else if (filterUI=="segment/audience" & id=="(All)" & !(textstr=="(All)")) {
  sid<-get_SID_list("/data/out/tables/segments_by_PID.csv")%>%filter(str_detect(Audience_Name, fixed(textstr,ignore_case=TRUE)))%>%select(1)
  sid<-as.numeric(as.character(sid$Audience_ID)) 
}else {
  sid<-get_Id_list("/data/out/tables/segments_by_PID.csv")
}

## get main table - DATAUSAGE
suppressMessages(get_report_datausage(endpoint="/v2/dmp/reports/datausage"))

## get other supplementary tables
suppressMessages(get_report(endpoint="/v2/dmp/reports/billing/overall"))
suppressMessages(get_report_audience(endpoint="/v2/dmp/reports/audience",pid))
#suppressMessages(get_report_dcId(endpoint="/v1/dmp/dataconsumers/{dataConsumerId}/segments",dcid))

# run only if sid (list of segment Ids) is not empty
sink(NULL)
if (length(sid)>=1){
  # get_report_SId(endpoint="/v2/dmp/segments/{segmentId}/audience",sid)
  get_report_SId(endpoint="/v1/dmp/segments/{segmentId}/audience/comparison",sid)
  get_report_SId(endpoint="/v1/dmp/segments/{segmentId}/audience/dynamics",sid)
  get_report_SId(endpoint="/v1/dmp/segments/{segmentId}/audience/totals",sid)
} else {
  #Writing a message to the console
  write(paste0("No specific segment / audience Id selected. Reports {segmentId}/comparison, dynamics, totals are not downloaded."), stdout())
}

# get segment groups
# suppressMessages(get_segmentGr_names(endpoint="/v2/dmp/reports/datausage"))                                 
