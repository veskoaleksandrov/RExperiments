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
ga_profiles <- NULL
for(j in 1:length(config$tokenpath)) {
  print(config$merchant[j])
  token <- authorize(cache = config$tokenpath[j])
  df <- list_profiles()
  df$merchant <- config$merchant[j]
  ga_profiles <- rbind.fill(ga_profiles, df)
}

# MS SQL Server 
conn <- odbcDriverConnect("Driver={SQL Server}; Server=192.168.10.64; Database=SandBox;")

# truncate table
sqlClear(channel = conn,
         sqtable = "dbo.AUX_GAviews", 
         errors = FALSE)

# write data frame to a table
sqlSave(channel = conn,
        dat = ga_profiles,
        tablename = "dbo.AUX_GAviews",
        rownames = FALSE, 
        varTypes = c(created = "datetime", 
                     updated = "datetime"), 
        append = TRUE)

# close all DB connections
odbcCloseAll()