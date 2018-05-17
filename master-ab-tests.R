if (!require("plyr")) install.packages("plyr", repos = "https://cloud.r-project.org/")
stopifnot(library(plyr, logical.return = TRUE))
if (!require("RGA")) install.packages("RGA", repos = "https://cloud.r-project.org/")
stopifnot(library(RGA, logical.return = TRUE, lib.loc = "O:/Lulu DWH/Scripts/R Scripts/Library"))
if (!require("RODBC")) install.packages("RODBC", repos = "https://cloud.r-project.org/")
stopifnot(library(RODBC, logical.return = TRUE))

#------ Config ----------------------------------------------------------------------------------------------------------------- 
config <- data.frame(metrics = c("ga:users"), 
                     dimensions = c("ga:date, ga:experimentId, ga:experimentVariant, ga:campaign, ga:countryIsoCode"), 
                     connection = c("Driver={SQL Server}; Server=192.168.10.64; Database=SandBox;"), 
                     stringsAsFactors = FALSE)

tokenpath <- c(`LULU Software` = "O:/Lulu DWH/Scripts/R Scripts/Library/Authorisation Tokens/ga.lulu.token", 
               `Interactive Brands` = "O:/Lulu DWH/Scripts/R Scripts/Library/Authorisation Tokens/ga.ib.token")

#------ DWH ----------------------------------------------------------------------------------------------------------------- 
conn <- odbcDriverConnect(config$connection[1])

# fetch experiments
ga_experiments <- sqlFetch(channel = conn, 
                           sqtable = "AUX_GAexperiments", 
                           stringsAsFactors = FALSE)

ga_experiments$starttime <- as.Date(ga_experiments$starttime, tz = "America/Montreal")
ga_experiments$endtime <- as.Date(ga_experiments$endtime, tz = "America/Montreal")

# keep RUNNING, recently ENDED experiments, exclude started today
ga_experiments <- ga_experiments[(ga_experiments$status == 'RUNNING' & as.Date(ga_experiments$starttime) < Sys.Date()) | 
                                   (ga_experiments$status == 'ENDED' & Sys.Date() - as.Date(ga_experiments$endtime) <= 3), ]

# fetch profiles
ga_profiles <- sqlFetch(channel = conn, 
                        sqtable = "AUX_GAviews", 
                        stringsAsFactors = FALSE)

# fetch goals
ga_goals <- sqlFetch(channel = conn, 
                     sqtable = "AUX_GAgoals", 
                     stringsAsFactors = FALSE)
ga_goals <- ga_goals[ga_goals$active == TRUE, ]

# loop through experiments
for(i in 1:nrow(ga_experiments)) {
  ga_e <- ga_experiments[i, ]
  print(paste0("Working on Experiment ID: ", ga_e$id[1]))
  ga_p <- ga_profiles[ga_profiles$id == ga_e$profileid[1], ]
  ga_g <- ga_goals[ga_goals$profileid == ga_e$profileid[1], ]
  
  downloadsGoalIndex <- subset(ga_g, subset = tolower(name) == "downloads")$id
  installsGoalIndex <- subset(ga_g, subset = tolower(name) == "installs")$id
  joinsGoalIndex <- subset(ga_g, subset = tolower(name) == "joins")$id
  checkoutsGoalIndex <- subset(ga_g, subset = tolower(name) == "checkouts")$id
  
  # update metrics
  goalsTable <- list(downloadsGoalIndex = downloadsGoalIndex, 
                     installsGoalIndex = installsGoalIndex, 
                     joinsGoalIndex = joinsGoalIndex, 
                     checkoutsGoalIndex = checkoutsGoalIndex)
  metrics <- config$metrics
  for(g in 1:length(goalsTable)) {
    if(!is.na(goalsTable[[g]] >= 1 && goalsTable[[g]] <= 25)) {
      metrics <- paste0(metrics, 
                        ", ga:goal", goalsTable[[g]], "Completions")
    }
  }
  
  #------ Google Analytics ----------------------------------------------------------------------------------------------------------------- 
  if(ga_e$merchant[1] == "LULU Software") {
    token <- authorize(cache = tokenpath["LULU Software"])
  }
  if(ga_e$merchant[1] == "Interactive Brands") {
    token <- authorize(cache = tokenpath["Interactive Brands"])
  }
  
  # list variants
  variation_names <-  get_experiment(account.id = ga_e$accountid[1], 
                                     webproperty.id = ga_e$webpropertyid[1], 
                                     profile.id = ga_e$profileid[1], 
                                     experiment.id = ga_e$id[1])$variations$name
  
  # determine experiment duration based on meta data from Analytics
  if(!is.na(ga_e$starttime[1])) {
    config$from <- as.Date(ga_e$starttime[1])
  } 
  if(!is.na(ga_e$endtime[1])) {
    config$to <- as.Date(ga_e$endtime[1])
  } else {
    config$to <- as.Date(Sys.Date() - 1)
  }
  stopifnot(config$to >= config$from)
  
  # loop through each day
  days <- seq(from = config$from[1], 
              to = config$to[1], 
              by = "days")
  ga_experiment_results <- NULL
  ga_experiment_transactions <- NULL
  for(j in seq_along(days)) {
    print(paste0("Fetching ", days[j]))
    # fetch data from Analytics, without segments
    temp <- get_ga(profile.id = ga_e$profileid[1],
                   start.date = days[j],
                   end.date = days[j],
                   metrics = metrics[1],
                   dimensions = config$dimensions[1],
                   filters = paste0("ga:experimentId==", ga_e$id[1]),
                   sampling.level = "higher_precision")
    if(!is.null(temp)) {
      ga_experiment_results <- rbind.fill(ga_experiment_results, temp)
      ga_experiment_results$Segment <- "all"
    }
    
    temp <- NULL
    # fetch transactions data from Analytics, without segments
    temp <- get_ga(profile.id = ga_e$profileid[1], 
                                         start.date = days[j], 
                                         end.date = days[j], 
                                         metrics = "ga:transactionRevenue", 
                                         dimensions = "ga:experimentId, ga:experimentVariant, ga:transactionId", 
                                         filters = paste0("ga:experimentId==", ga_e$id[1]), 
                                         sampling.level = "higher_precision")
    if(!is.null(temp)) {
      temp$date <- days[j]
      ga_experiment_transactions <- rbind.fill(ga_experiment_transactions, temp)
      ga_experiment_transactions$Segment <- "all"
    }

    rm(temp)
  }
  
  if(!is.null(ga_experiment_results)) {
    # rename columns
    names(ga_experiment_results)[names(ga_experiment_results) == "date"] <- "Date"
    names(ga_experiment_results)[names(ga_experiment_results) == "campaign"] <- "Campaign"
    names(ga_experiment_results)[names(ga_experiment_results) == "country.iso.code"] <- "CountryISO"
    names(ga_experiment_results)[names(ga_experiment_results) == "users"] <- "Users"
    names(ga_experiment_results)[names(ga_experiment_results) == paste0("goal", downloadsGoalIndex, "Completions")] <- "Downloads"
    names(ga_experiment_results)[names(ga_experiment_results) == paste0("goal", installsGoalIndex, "Completions")] <- "Installs"
    names(ga_experiment_results)[names(ga_experiment_results) == paste0("goal", joinsGoalIndex, "Completions")] <- "Joins"
    names(ga_experiment_results)[names(ga_experiment_results) == paste0("goal", checkoutsGoalIndex, "Completions")] <- "Checkouts" 
  }
  
  #------ Google Analytics Transaction IDs ----------------------------------------------------------------------------------------------------------------- 
  
  if(!is.null(ga_experiment_transactions)){
    ga_experiment_transactions$Segment <- "all"
    ga_experiment_transactions <- subset(x = ga_experiment_transactions, select = -c(transaction.revenue))
    
    # identify duplicate transactions (same transaction appearing in several segments)
    dups <- ga_experiment_transactions[duplicated(ga_experiment_transactions$transaction.id),]
    # print(paste0("duplicate transactions [%] = ", 
    #              100 * length(dups$transaction.id) / length(ga_experiment_transactions$transaction.id)))
    
    if(!is.null(dups) & nrow(dups) > 0) {
      # keep only the first occurance of each transaction id
      for(k in 1:nrow(dups)) {
        df <- ga_experiment_transactions[ga_experiment_transactions$transaction.id == dups$transaction.id[k],]
        firstdate <- min(df$date)
        ga_experiment_transactions <- ga_experiment_transactions[!(ga_experiment_transactions$transaction.id == dups$transaction.id[k] & ga_experiment_transactions$date != firstdate),]
        rm(df, firstdate)
      }
    }
    # delete date
    ga_experiment_transactions <- subset(x = ga_experiment_transactions, select = -c(date))
  } else {
    ga_experiment_transactions <- NULL
    ga_experiment_transactions$`transaction.id` <- NA
  }
  
  #------ Upclick ----------------------------------------------------------------------------------------------------------------- 
  print("Getting Upclick Transactions data")
  conn <- odbcDriverConnect(config$connection[1])
  uc_transactions <- sqlQuery(channel = conn, 
                              query = paste0("SELECT * FROM [Lulu_DWH].[dbo].[AUX_Upclick_Transactions] WHERE [GlobalOrderID] IN (", paste0("'", unique(ga_experiment_transactions$transaction.id), "'", collapse = ", "), ")"), 
                              stringsAsFactors = FALSE, 
                              na.strings = c("", "N/A", "NA"))
  
  #------- PayPal -----------------------------------------------------------------------------------------------------------------
  print("Getting PayPal Transactions data")
  conn <- odbcDriverConnect(config$connection[1])
  pp_transactions <- sqlQuery(channel = conn,
                              query = paste0("SELECT * FROM [SandBox].[dbo].[AUX_PayPal] WHERE [TransactionID] IN (", paste0("'", unique(ga_experiment_transactions$transaction.id), "'", collapse = ", "), ")"), 
                              stringsAsFactors = FALSE,
                              na.strings = c("", "N/A", "NA"))

  # sync PayPal column names with Upclick column names
  names(pp_transactions)[names(pp_transactions) == "Type"] <- "TransactionType"
  names(pp_transactions)[names(pp_transactions) == "TransactionID"] <- "GlobalOrderID"
  names(pp_transactions)[names(pp_transactions) == "Date"] <- "TransactionDate"
  names(pp_transactions)[names(pp_transactions) == "FirstName"] <- "Firstname"
  names(pp_transactions)[names(pp_transactions) == "LastName"] <- "Lastname"
  names(pp_transactions)[names(pp_transactions) == "Currency"] <- "CurrencyISO"
  names(pp_transactions)[names(pp_transactions) == "SalesTaxAmount"] <- "Tax"
  names(pp_transactions)[names(pp_transactions) == "GrossAmount"] <- "ProductPriceUSD"
  names(pp_transactions)[names(pp_transactions) == "ItemNumber"] <- "mSKU"
  names(pp_transactions)[names(pp_transactions) == "ItemName"] <- "ProductTitle"
  names(pp_transactions)[names(pp_transactions) == "Tax"] <- "TaxAmountUSD"
  if(!is.null(pp_transactions) & nrow(pp_transactions) > 0) {
    pp_transactions$ProductLevel <- "MainProduct"
    pp_transactions$TaxAmountUSD <- abs(pp_transactions$TaxAmountUSD)
    pp_transactions$Fees <- abs(pp_transactions$Fees)
  }
  
  # remove the not required columns
  pp_transactions <- subset(x = pp_transactions, select = -c(NetAmount,
                                                             # Type,
                                                             ShippingStateProvince,
                                                             ShippingZipPostalCode,
                                                             ShippingAddressCountry,
                                                             ShippingMethod,
                                                             Name,
                                                             ToEmail,
                                                             BillingAddressLine1,
                                                             BillingAddressLine2,
                                                             BillingAddressCity,
                                                             BillingStateProvince,
                                                             BillingZipPostalCode,
                                                             BillingAddressCountry,
                                                             StoreID,
                                                             TerminalID,
                                                             ReferenceTransactionID,
                                                             ShippingName,
                                                             ShippingAddressLine1,
                                                             ShippingAddressLine2,
                                                             ShippingAddressCity,
                                                             AddressStatus,
                                                             ContactPhoneNumber,
                                                             ReceiptID,
                                                             CustomField,
                                                             Option1Name,
                                                             Option1Value,
                                                             Option2Name,
                                                             Option2Value,
                                                             Note,
                                                             AuctionSite,
                                                             AuctionUserID,
                                                             ItemURL,
                                                             AuctionClosingDate,
                                                             InsuranceAmount,
                                                             ShippingHandlingAmount,
                                                             Time,
                                                             TimeZone,
                                                             InvoiceID, 
                                                             Subject, 
                                                             Status, 
                                                             Balance, 
                                                             BalanceImpact, 
                                                             CountryCode))

  # union Upclick and PayPal
  uc_transactions <- rbind.fill(uc_transactions, pp_transactions)
  
  # keep only the relevant GlobalOrderIDs
  uc_experiment_transactions <- merge(x = uc_transactions, 
                                      y = ga_experiment_transactions, 
                                      by.x = "GlobalOrderID", 
                                      by.y = "transaction.id", 
                                      all.y = TRUE)
  
  # remove test transactions
  uc_experiment_transactions <- uc_experiment_transactions[!grepl(pattern = "lulusoftware.com|test.com", x = uc_experiment_transactions$Email, ignore.case = TRUE),]
  uc_experiment_transactions <- uc_experiment_transactions[!grepl(pattern = "^(dbc|test|testing|tester)$", x = uc_experiment_transactions$Firstname, ignore.case = TRUE),]
  uc_experiment_transactions <- uc_experiment_transactions[!grepl(pattern = "^(dbc|test|testing|tester)$", x = uc_experiment_transactions$Lastname, ignore.case = TRUE),]
  
  # remove transactions where email is NA
  uc_experiment_transactions <- uc_experiment_transactions[!is.na(uc_experiment_transactions$Email), ]
  
  # black list transactions with type other than Sale & Recurring
  uc_experiment_transactions_blacklist <- unique(subset(uc_experiment_transactions, 
                                                        TransactionType != "Sale" & 
                                                          TransactionType != "Recurring" & 
                                                          TransactionType != "Rebill" & 
                                                          TransactionType != "Express Checkout Payment", 
                                                        select = GlobalOrderID))
  # black list transactions with quantity greater than 1
  uc_experiment_transactions_blacklist <- rbind.fill(uc_experiment_transactions_blacklist, 
                                                     unique(subset(x = uc_experiment_transactions, subset = Quantity > 1, select = GlobalOrderID)))
  # filter out the black list
  uc_experiment_transactions <- subset(uc_experiment_transactions, !(GlobalOrderID %in% uc_experiment_transactions_blacklist$GlobalOrderID))
  
  # add main product field
  mp <- subset(x = uc_experiment_transactions, 
               subset = ProductLevel == "MainProduct", 
               select = c(GlobalOrderID, ProductTitle))
  names(mp)[names(mp) == "ProductTitle"] <- "MainProduct"
  mp <- unique(mp)
  
  uc_experiment_transactions <- merge(x = uc_experiment_transactions, 
                                      y = mp, 
                                      by.x = "GlobalOrderID", 
                                      by.y = "GlobalOrderID", 
                                      all.x = TRUE)
  
  # add subscription model field
  if(length(uc_experiment_transactions$NextRebillDate) > 0) {
    uc_experiment_transactions$SubscriptionModel <- paste0(as.Date(uc_experiment_transactions$NextRebillDate) - as.Date(uc_experiment_transactions$TransactionDate), " days")
  } else {
    uc_experiment_transactions$SubscriptionModel <- character()
  }
  
  # remove columns
  uc_experiment_transactions <- subset(x = uc_experiment_transactions, select = -c(ProductPrice, TaxAmount))
  
  # rename columns
  names(uc_experiment_transactions)[names(uc_experiment_transactions) == "cmp"] <- "Campaign"
  names(uc_experiment_transactions)[names(uc_experiment_transactions) == "TransactionDate"] <- "Date"
  names(uc_experiment_transactions)[names(uc_experiment_transactions) == "ProductPriceUSD"] <- "Revenue"
  names(uc_experiment_transactions)[names(uc_experiment_transactions) == "TaxAmountUSD"] <- "Tax"
  
  # union traffic and transactions data sets 
  report <- rbind.fill(ga_experiment_results, uc_experiment_transactions)
  
  # convert to lowercase
  report$CountryISO <- tolower(report$CountryISO)
  report$Campaign <- tolower(report$Campaign)
  
  # force data types
  report$Revenue <- as.numeric(report$Revenue)
  report$NextRebillDate <- as.Date(report$NextRebillDate)
  report$Tax <- as.numeric(report$Tax)
  report$Fees <- as.numeric(report$Fees)
  report$Quantity <- as.integer(report$Quantity)
  
  # rename columns 
  names(report)[names(report) == "experiment.id"] <- "ExperimentID"
  names(report)[names(report) == "experiment.variant"] <- "VariationID"
  
  # add Variation name
  report$`Variation name` <- variation_names[report$VariationID + 1]
  
  if(!is.null(report) & nrow(report) > 0) {
    # add Experiment name 
    report$Name <- ga_e$name[1] 
    
    #------- Load into DWH -----------------------------------------------------------------------------------------------------------------
    # delete existing data for current experiment
    print(paste0("Deleting records for ", ga_e$id[1]))
    conn <- odbcDriverConnect(config$connection[1])
    sqlQuery(channel = conn, 
             query = paste0("DELETE FROM [SandBox].[dbo].[AUX_GAexperiments_data] WHERE ExperimentID = '", ga_e$id[1], "'"))
    
    # add null columns
    if(is.null(report$Downloads)) {
      report$Downloads <- as.integer(NA)
    }
    if(is.null(report$Installs)) {
      report$Installs <- as.integer(NA)
    }
    if(is.null(report$Joins)) {
      report$Joins <- as.integer(NA)
    }
    if(is.null(report$Checkouts)) {
      report$Checkouts <- as.integer(NA)
    }
    
    # append data
    print(paste0("Inserting records for ", ga_e$id[1]))
    conn <- odbcDriverConnect(config$connection[1])
    sqlSave(channel = conn, 
            dat = report, 
            tablename = "AUX_GAexperiments_data", 
            rownames = FALSE, 
            varTypes = c(Date = "date", NextRebillDate = "date"), 
            append = TRUE)
  }
}

# close all DB connections
print("Closing SQL connection.")
odbcCloseAll()