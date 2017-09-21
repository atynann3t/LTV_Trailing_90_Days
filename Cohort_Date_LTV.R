
require(bigrquery)

project <- '610510520745'
args <- commandArgs(trailingOnly = TRUE)
set_service_token(args[1])

require(binhf)
require(plyr)
require(dplyr)
require(scales)
require(flexdashboard)
require(drc)
require(nlme)
library(ggplot2)
library(forecast)
library(xlsx)
library(tibble)
library(formattable)
library(broom)


# date_range <- "SELECT DATEDIFF(CURRENT_DATE(),'2016-10-01')"
# date_range <- paste("Select DATEDIFF(CURRENT_DATE(),DATE(DATE_ADD(DATE(current_date()), (",day_num,"*-1), 'DAY')))",sep='') 
# date_range_query_execute <- query_exec(date_range, project = project)
# cohort_days <- date_range_query_execute[1,1]

cohort_days <- 97

###########
select_revenue <- "SELECT joindate as date, SUM(CASE WHEN playerage <= 0 AND DATEDIFF(max_date,joindate) >= 0 THEN net_revenue ELSE NULL END) AS d0_ARPU,"
body_revenue <- character()
for(i in 1:cohort_days) {
  body_revenue[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND DATEDIFF(max_date,joindate) >= ",i," THEN net_revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}
body_revenue <- paste(body_revenue,collapse = " ")
source('query_body.R') # insert day_num into the body of the query 
end_revenue <- query_body # update 
end_revenue <- gsub("[\r\n]", " ", end_revenue) 
org_revenue <- paste(select_revenue,body_revenue,end_revenue,sep = ' ')
revenue_query_execute <- query_exec(org_revenue, project = project)
revenue_data <- revenue_query_execute

revenue_data %>%  dim()
View(revenue_data)

#### ua retention curve
select_ua_retention_curve <- "SELECT joindate AS date, EXACT_COUNT_DISTINCT(CASE WHEN playerage = 0 AND channel != 'organic' THEN gamerid ELSE NULL END) AS installs,"
body_ua_retention_curve <- character()
for(i in 1:cohort_days) {
  body_ua_retention_curve[i] <- paste("EXACT_COUNT_DISTINCT(CASE WHEN playerage = ",i," AND channel != 'organic' THEN gamerid ELSE NULL END) AS d",i,",",sep='')
}
body_ua_retention_curve <- paste(body_ua_retention_curve,collapse = " ")
end_ua_retention_curve <- query_body # update 
end_ua_retention_curve <- gsub("[\r\n]", " ", end_ua_retention_curve) 
sql <- paste(select_ua_retention_curve,body_ua_retention_curve,end_ua_retention_curve,sep = ' ')
query_execute <- query_exec(sql, project = project)
data <- query_execute

##### captures data only (by removing the first column of dates)
x <- data[,c(2:ncol(data))]

#### overwrites unneeded installs
x[2:nrow(x),1] <- 0  # this seems like it should be: x[1:nrow(x),1] <- 0

##### pivots for retention curve
mat <- as.matrix(x)
for (i in 3:nrow(mat)) {
  mat[i,] <- shift(mat[i,],i-2)
}

##### pivoted matrix
matrix_size <- nrow(mat) - 1
retention_curve <- mat[c(1:matrix_size),c(1:matrix_size)]

##### captures date range
dates <- c(data[c(1:(nrow(data)-1)),1])

#### reassigns retention curve to dates
colnames(retention_curve) <- dates

##### finds retention curve sums
retention_curve_sums <- colSums(retention_curve)

### assigns as a dataframe
retention_curve_sums <- as.data.frame(retention_curve_sums)
retention_curve_sums$ua_installs <- data[1:nrow(retention_curve_sums),2]

#### dau sql
  # CHANGED  date >= '2016-10-01' to cast(date as timestamp) >= DATE_ADD(CURRENT_DATE(), -97, 'DAY')
dau_sql <-"SELECT date 
                 ,EXACT_COUNT_DISTINCT(gamerid) AS DAU 
              FROM [n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary] 
            WHERE cast(date as timestamp) >= DATE_ADD(CURRENT_DATE(), -97, 'DAY')
          GROUP BY 1 
          ORDER BY 1"
dau_query_execute <- query_exec(dau_sql, project = project)
dau_data <- dau_query_execute

## pull organic installs 
source('org_install_query.R')
org_install_query <- gsub("[\r\n]", " ", org_install_query) 
org_install_query_execute <- query_exec(org_install_query, project = project)
org_install_data <- org_install_query_execute

###### orgdau calculation
orgdau <- org_install_data$installs/ dau_data$DAU
orgdau <- orgdau[1:(length(orgdau)-1)]

###### merging into ua retention curve sums
retention_curve_sums$orgdau <- orgdau
retention_curve_sums$org_installs_from_ua <- round(retention_curve_sums$orgdau*retention_curve_sums$retention_curve_sums,digits = 0)

##### calculations for ua percentage of organic revenue
retention_curve_sums$org_installs <- org_install_data[,2][1:(nrow(retention_curve_sums))]
retention_curve_sums$ua_perc_for_rev <- retention_curve_sums$org_installs_from_ua / retention_curve_sums$org_installs

# pull organic rev 
select_org_rev <- "SELECT joindate as date, SUM(CASE WHEN playerage = 0 AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,"
body_org_rev <- character()
for(i in 1:cohort_days) {
  body_org_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND (channel = 'organic' ) AND DATEDIFF(max_date,joindate) >= ",i," THEN revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}
body_org_rev <- paste(body_org_rev,collapse = " ")
end_org_rev <- query_body # from source('query_body.R') 
end_org_rev <- gsub("[\r\n]", " ", end_org_rev) 
org_rev_sql <- paste(select_org_rev,body_org_rev,end_org_rev,sep = ' ')
org_rev_query_execute <- query_exec(org_rev_sql, project = project)
org_rev_data <- org_rev_query_execute

org_rev_data %>% dim()
View(org_rev_data)

# pull spend 
spend_sql <- "SELECT date
                    ,SUM(spend) AS spend 
                  FROM (SELECT date
                              ,campaign_name
                              ,ad_network
                              ,platform
                              ,spend 
                            FROM [n3twork-marketing-analytics:INSTALL_ATTRIBUTION.tenjin_summary] 
                          WHERE date >= '2016-10-01'
                          AND bundle_id = 'com.n3twork.legendary'
                          and cast(date as timestamp) >= DATE_ADD(CURRENT_DATE(), -97, 'DAY')
                        GROUP BY 1, 2, 3, 4, 5 
                        ORDER BY 1
                      ) 
              GROUP BY 1 
              ORDER BY 1"
spend_query_execute <- query_exec(spend_sql, project = project)
spend_data <- spend_query_execute

#### pairs down to match size and only cohorts that have a posted d7
retention_curve_sums <- retention_curve_sums[1:(nrow(retention_curve_sums)-6),]
organic_revenue_date_match <- org_rev_data[1:nrow(retention_curve_sums),]

spend_data_date_match <- spend_data[1:nrow(retention_curve_sums),]
retention_curve_sums$spend <- spend_data_date_match$spend
retention_curve_sums$aCPI <- retention_curve_sums$spend / (retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua)
retention_curve_sums$eCPI <- retention_curve_sums$spend / (retention_curve_sums$ua_installs + retention_curve_sums$org_installs)
##### calculates revenue from organic revenue from ua_contribution
percent_ua_contribution <- retention_curve_sums$ua_perc_for_rev
organic_revenue_ua_contribution <- organic_revenue_date_match[,2:ncol(organic_revenue_date_match)]
#### for aCPI
# organic_revenue_ua_contribution <- organic_revenue_ua_contribution*percent_ua_contribution
###### for eCPI
organic_revenue_ua_contribution <- organic_revenue_ua_contribution
##### adds back in dates
dates <- organic_revenue_date_match[,1]
organic_revenue_ua_contribution <- cbind(dates,organic_revenue_ua_contribution)

#### query exectute for ua revenue
select_ua_rev <- "SELECT joindate AS date, SUM(CASE WHEN playerage <= 0 AND (channel != 'organic') AND DATEDIFF(max_date,joindate) >= 0 THEN revenue ELSE NULL END) AS d0_ARPU,"
body_ua_rev <- character()
for(i in 1:cohort_days) {
  body_ua_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND (channel != 'organic' ) AND DATEDIFF(max_date,joindate) >= ",i," THEN revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
}
body_ua_rev <- paste(body_ua_rev,collapse = " ")
end_ua_rev <- query_body # from source('query_body.R') 
end_ua_rev <- gsub("[\r\n]", " ", end_ua_rev) 
ua_rev_sql <- paste(select_ua_rev,body_ua_rev,end_ua_rev,sep = ' ')
ua_rev_query_execute <- query_exec(ua_rev_sql, project = project)
ua_rev_data <- ua_rev_query_execute

org_rev_data %>% dim()
View(org_rev_data)


#### matches date range for cohorts with 7 days baked in
ua_revenue_date_match <- ua_rev_data[1:nrow(retention_curve_sums),]

#### blends ua revenue and organic revenue contribution
total_ua_revenue <- organic_revenue_ua_contribution[,2:ncol(organic_revenue_ua_contribution)] + ua_revenue_date_match[,2:ncol(ua_revenue_date_match)]
total_ua_revenue <- cbind(dates,total_ua_revenue)

#### retrieves total ua installs with k-factor for aCPI
# total_ua_installs <- retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua
#### for eCPI
total_ua_installs <- retention_curve_sums$ua_installs + retention_curve_sums$org_installs

#### calculates arpi
total_arpi <- total_ua_revenue[,2:ncol(total_ua_revenue)] / total_ua_installs

View(total_arpi)
total_arpi %>% dim()
total_arpi[c(1:5),c(2:5)]

total_arpi_AJT <- total_arpi[,c(2:ncol(total_arpi))]
total_arpi_AJT_means <- colMeans(total_arpi_AJT, na.rm = TRUE)
total_arpi_AJT_means <- total_arpi_AJT_means[1:97]
total_arpi_AJT_means %>% class()
total_arpi_AJT_means %>% length()

##### creates extended forecasting range
df <- data.frame(matrix(NA, ncol = 366 - ncol(total_arpi), nrow = nrow(total_arpi)))

df_column_names <- character()
for(i in ncol(total_arpi):365) {
  df_column_names[i] <- paste("d",i,"_ARPU",sep='')
}
df_column_names <- df_column_names[!is.na(df_column_names)]
colnames(df) <- df_column_names
length(df_column_names)

total_arpi <- cbind(dates,total_arpi,df)

View(total_arpi)

##### transposes cohorts to dataframe in order to support prediction formats
total_arpi_transposed <- t(total_arpi)
cohort_days <- c(0:365)
colnames(total_arpi_transposed) <- total_arpi_transposed[1, ]
total_arpi_transposed <- as.data.frame(total_arpi_transposed[-1,])  

total_arpi_transposed <- cbind(cohort_days,total_arpi_transposed)

#### models the data for each cohort
for (i in 2:ncol(total_arpi_transposed)) {
  a <- as.numeric(levels(total_arpi_transposed[,i]))
  a <- sort(a) 
  b <- total_arpi_transposed[1:length(a),1]
  prediction_set <- cbind(b,a)  
  prediction_set <- as.data.frame(prediction_set)
  assign(paste("fit",i,sep=''),
         nls(a ~ SSgompertz(log(b+1), A, B, C) ,
             data = prediction_set,
             start = list(A=40000,B=11.8,C=.92),
             upper = list(A=40000,B=13,C=.93),
             lower = list(A=40000,B=11.0,C=.90),
             algorithm = "port",
             control = nls.control(maxiter = 500000,tol = 1e-09,warnOnly=T))
  )
}

# AJT 
a <- sort(total_arpi_AJT_means) 
b <- c(0:96)
prediction_set <- cbind(b,a)  
prediction_set <- as.data.frame(prediction_set)
prediction_set %>% head()

# model 
model_AJT <- 
nls(a ~ SSgompertz(log(b+1), A, B, C) ,
     data = prediction_set,
     start = list(A=40000,B=11.8,C=.92),
     upper = list(A=40000,B=13,C=.93),
     lower = list(A=40000,B=11.0,C=.90),
     algorithm = "port",
     control = nls.control(maxiter = 500000,tol = 1e-09,warnOnly=T))

# get each coef
A <- tidy(model_AJT)[1,2]
B <- tidy(model_AJT)[2,2]
C <- tidy(model_AJT)[3,2]
# insert the coef into the formula 
equation <- paste(as.character(A),"*exp(-",as.character(B),"*",as.character(C),"^log(90))",sep='')

rm(ltv_output)
ltv_output <- cbind(as.character("No Country")
                    ,equation
                    ,as.character("No Channel")
                    ,as.character("No Channel"))
ltv_output <- cbind(as.data.frame(as.character(Sys.time())),ltv_output)
colnames(ltv_output) <- c("created_timestamp", "county", "equation", "channel", "platform")

ltv_output <- data.frame(lapply(ltv_output, as.character), stringsAsFactors=FALSE)
lapply(ltv_output, class) # check 
ltv_output %>% dim()
ltv_output %>% head()

# test_ltv_profiles_tbl <-get_table(project = '167707601509', dataset = "TEST_AGAMEMNON", table = "test_ltv_profiles")

# pulled Martin's data to reload it, not actually using 
# test_ltv_profiles_sql <- "Select * from [n3twork-marketing-analytics:TEST_AGAMEMNON.test_ltv_profiles]"
# test_ltv_profiles_tbl <- query_exec(test_ltv_profiles_sql, project = project)

# load Martin's data and update it 
# setwd("/Users/andrewtynan/Desktop")
# Martin_Table_Example <- read.csv("Martin_Table_Example.csv", sep = ",")
# Martin_Table_Example %>%  head()
# update the equation 
# Martin_Table_Example[["equation"]] <- "4.000e+04*exp(-1.298e+01*9.192e-01^log(90))"
# check that it updated 
# Martin_Table_Example %>%  head()
#update the table in BigQuery
insert_upload_job(project = '167707601509', 
                  dataset = "TEST_AGAMEMNON", 
                  table = "test_ltv_profiles",  
                  values = ltv_output, 
                  write_disposition = "WRITE_APPEND")   




finished_matrix <- as.matrix(total_arpi_transposed,stringsAsFactors= F)

#### pulls model coefficients and stores each cohort to a dataframe
model.list<-mget(grep("fit[0-9]+$", ls(),value=T))
coefs<-lapply(model.list,function(x)coef(x))
flattened_coefs <- as.data.frame(unlist(coefs))
colnames(flattened_coefs) <- c('coefficients')
flattened_coefs$index <- row.names(flattened_coefs)
flattened_coefs$index <- as.numeric(gsub('[a-zA-Z.]','',flattened_coefs$index))
flattened_coefs$model_component <- gsub('[a-z0-9.]','',row.names(flattened_coefs))

####### Fills in na's with predicted values
for (z in 2:ncol(finished_matrix)) {
  A <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'A',1]
  B <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'B',1]
  C <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'C',1]
  for (i in 1:nrow(finished_matrix)){
    if (is.na(finished_matrix[i,z])) {
      finished_matrix[i,z] <- A*exp(-B*C^log(as.numeric(finished_matrix[i,1])))
    }
  }
}


#### writes out final output
transposed_predictions <- t(finished_matrix)
myData <- as.data.frame(transposed_predictions[-c(1), ], stringsAsFactors = F)
cohort_names <- row.names(myData)
myData <- sapply( myData, as.numeric )
myDataNet <- myData*.7
myData <- as.data.frame(myData)
myDataNet <- as.data.frame(myDataNet)
rownames(myData) <- cohort_names
rownames(myDataNet) <- cohort_names

base_info <- cbind(retention_curve_sums$eCPI,
                   retention_curve_sums$ua_installs + retention_curve_sums$org_installs,
                   myData$d7_ARPU / retention_curve_sums$eCPI,
                   myData$d30_ARPU / retention_curve_sums$eCPI,
                   myData$d90_ARPU / retention_curve_sums$eCPI)
colnames(base_info) <- c('eCPI','Installs','d7 ROI','d30 ROI','d90 ROI')
final_output <- cbind(base_info,myData)


final_output <-  final_output %>%
  arrange(desc(rownames(final_output)))
rownames(final_output) <- rev(cohort_names)
days_to_backout <- NA 
for (i in 1:nrow(final_output)) {
  days_to_backout[i] <- which.min(final_output[i,'eCPI'] > final_output[i,6:ncol(final_output)])
}
days_to_backout
current_yield <- as.numeric(diag(as.matrix(final_output[,13:ncol(final_output)]))) / final_output$eCPI
final_output$current_yield <- current_yield
final_output$days_to_backout <- days_to_backout



final_output[,1] <- dollar_format()(final_output[,1])
final_output[,3] <- percent((final_output[,3]))
final_output[,4] <- percent((final_output[,4]))
final_output[,5] <- percent((final_output[,5]))
final_output$current_yield <- percent(final_output$current_yield)

base_info_net <- cbind(retention_curve_sums$eCPI,
                       retention_curve_sums$ua_installs + retention_curve_sums$org_installs,
                       myDataNet$d7_ARPU / retention_curve_sums$eCPI,
                       myDataNet$d30_ARPU / retention_curve_sums$eCPI,
                       myDataNet$d90_ARPU / retention_curve_sums$eCPI)
colnames(base_info_net) <- c('eCPI','Installs','d7 ROI','d30 ROI','d90 ROI')
final_output_net <- cbind(base_info_net,myDataNet)

final_output_net <-  final_output_net %>%
  arrange(desc(rownames(final_output_net)))
rownames(final_output_net) <- rev(cohort_names)
days_to_backout_net <- NA 
for (i in 1:nrow(final_output_net)) {
  days_to_backout_net[i] <- which.min(final_output_net[i,'eCPI'] > final_output_net[i,6:ncol(final_output_net)])
}
current_yield_net <- as.numeric(diag(as.matrix(final_output_net[,13:ncol(final_output_net)]))) / final_output_net$eCPI
final_output_net$current_yield <- current_yield_net
final_output_net$days_to_backout <- days_to_backout_net

bq_table_output <- final_output_net[,c('eCPI','Installs','d7 ROI','d30 ROI','d90 ROI')]
bq_table_output <- bq_table_output %>%
  mutate(date = rownames(bq_table_output))
colnames(bq_table_output) <- c('eCPI','Installs','d7_ROI','d30_ROI','d90_ROI','date')
bq_table_output <- as.data.frame(bq_table_output)

insert_upload_job(project = '167707601509', 
                  dataset = "LEG_Predictions", 
                  table = "eCPI_LTV_total_portfolio",  
                  values = bq_table_output, 
                  write_disposition = "WRITE_TRUNCATE")                 

final_output_net[,1] <- dollar_format()(final_output_net[,1])
final_output_net[,3] <- percent((final_output_net[,3]))
final_output_net[,4] <- percent((final_output_net[,4]))
final_output_net[,5] <- percent((final_output_net[,5]))
final_output_net$current_yield <- percent(final_output_net$current_yield)

setwd("analytics_tools/marketing/Leg_R_Analysis/")
write.xlsx(final_output[,c("eCPI","Installs","days_to_backout","current_yield","d7 ROI","d30 ROI","d90 ROI")], file = 'predictions_eCPI.xlsx', sheetName = 'Gross ARPI Figures', row.names = T)
write.xlsx(final_output_net[,c("eCPI","Installs","days_to_backout","current_yield","d7 ROI","d30 ROI","d90 ROI")], file = 'predictions_eCPI.xlsx', sheetName = 'Net ARPI Figures', row.names = T, append = T)
write.csv(file='total_cohort_revenue.csv', revenue_data, row.names = F)
write.csv(final_output_net, file = 'uncapped_net_roi.csv',row.names = T)

