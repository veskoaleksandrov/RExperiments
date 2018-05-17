if (!require("plyr")) install.packages("plyr", repos = "https://cloud.r-project.org/")
stopifnot(library(plyr, logical.return = TRUE))
if (!require("RGA")) install.packages("RGA", repos = "https://cloud.r-project.org/")
stopifnot(library(RGA, logical.return = TRUE, lib.loc = "O:/Lulu DWH/Scripts/R Scripts/Library"))
if (!require("RODBC")) install.packages("RODBC", repos = "https://cloud.r-project.org/")
stopifnot(library(RODBC, logical.return = TRUE))

#------ Config ----------------------------------------------------------------------------------------------------------------- 
config <- data.frame(tokenpath = c("O:/Lulu DWH/Scripts/R Scripts/Library/Authorisation Tokens/ga.lulu.token", 
                                   "O:/Lulu DWH/Scripts/R Scripts/Library/Authorisation Tokens/ga.ib.token"), 
                     merchant = c("LULU Software", "Interactive Brands"), 
                     stringsAsFactors = FALSE)

#------ Google Analytics ----------------------------------------------------------------------------------------------------------------- 
ga_experiments <- NULL
for(j in 1:length(config$tokenpath)) {
  print(config$merchant[j])
  token <- authorize(cache = config$tokenpath[j])
  ga_profiles <- list_profiles()
  for(i in 1:nrow(ga_profiles)) {
    df <- list_experiments(account.id = ga_profiles$account.id[i], 
                           webproperty.id = ga_profiles$webproperty.id[i], 
                           profile.id = ga_profiles$id[i])
    if(!is.null(df)) {
      df$merchant <- config$merchant[j]
      df$profile.id <- ga_profiles$id[i]
      df$profile.name <- ga_profiles$name[i]
      ga_experiments <- rbind.fill(ga_experiments, df)
    }
  }
}

ga_experiments$start.time <- as.POSIXct(x = ga_experiments$start.time, format = "%Y-%m-%dT%H:%M:%OS")
ga_experiments$end.time <- as.POSIXct(x = ga_experiments$end.time, format = "%Y-%m-%dT%H:%M:%OS")

# MS SQL Server 
conn <- odbcDriverConnect("Driver={SQL Server}; Server=192.168.10.64; Database=SandBox;")

# truncate table
sqlClear(channel = conn,
         sqtable = "dbo.AUX_GAexperiments", 
         errors = FALSE)

# write data frame to a table
sqlSave(channel = conn,
        dat = ga_experiments,
        tablename = "dbo.AUX_GAexperiments",
        rownames = FALSE, 
        varTypes = c(created = "datetime", 
                     updated = "datetime", 
                     start.time = "datetime", 
                     end.time = "datetime"), 
        append = TRUE)

# close all DB connections
odbcCloseAll()