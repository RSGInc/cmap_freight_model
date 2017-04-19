##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       4_PMG_Outputs.R reads in a reassembles the outputs from the PMGs
#Date:              June 30, 2014
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.
##############################################################################################

#-----------------------------------------------------------------------------------
#Step 4 PMG Outputs
#-----------------------------------------------------------------------------------
progressStart(pmgout,3)

#-----------------------------------------------------------------------------------
#Combine the PMG Output files
#-----------------------------------------------------------------------------------
progressNextStep("Combining PMG Output files")

naicsrun <- 1L #counter
load(file.path(model$outputdir,"naics_set.Rdata"))
### -- Heither, 02-24-2017: testing: no 327390
#naics_set <- subset(naics_set, NAICS %in% c(324122,327400,339910,327310,327200,327993,327992,327999,327991,327100,327330))
naics_set <- subset(naics_set, NAICS %in% model$scenvars$pmgnaicstorun)
naicstorun <- nrow(naics_set)
naicspairs <- list() #list to hold the summarized outputs

debug <- TRUE
while (naicsrun <= naicstorun){

  #check if the next naics market is ready
  naicsLogPath <-file.path(model$outputdir,paste0(naics_set$NAICS[naicsrun],".txt"))
  fileExists <- file.exists(naicsLogPath)
  if (debug) print(paste("naicsLogPath:", naicsLogPath, "fileExists:", fileExists, "file.size:", file.size(naicsLogPath)))
  if (fileExists) {

    naicslog <- readLines(naicsLogPath)

    if(length(naicslog)>0){

      if(grepl("Completed Processing Outputs",naicslog[length(naicslog)],fixed=TRUE)){

        print(paste("Reading Outputs from Industry",naicsrun,":",naics_set$NAICS[naicsrun]))
        load(file.path(model$outputdir,paste0(naics_set$NAICS[naicsrun],".Rdata")))
        #apply fix for bit64/data.table handling of large integers in rbindlist
        pairs[,Quantity.Traded:=as.character(Quantity.Traded)]
        pairs[,Last.Iteration.Quantity:=as.character(Last.Iteration.Quantity)]
        naicspairs[[naicsrun]] <- pairs
        naicsrun <- naicsrun + 1L

      } else {
        Sys.sleep( 30 )
      }

    } else {
      Sys.sleep( 30 )
    }

  } else {
    Sys.sleep( 30 )
  }

}

pairs <- rbindlist(naicspairs)
rm(naicspairs)
#apply fix for bit64/data.table handling of large integers in rbindlist
pairs[,Quantity.Traded:=as.integer64.character(Quantity.Traded)]
pairs[,Last.Iteration.Quantity:=as.integer64.character(Last.Iteration.Quantity)]

pairs[,AnnualValue:=(Last.Iteration.Quantity/PurchaseAmountTons)*ConVal]	### Heither 10-05-2016, annual shipment value in dollars

## ---------------------------------------------------------------
## Heither, revised 01-29-2016: This code block checks the pairs file for ineligible modes selected between zone pairs.
setkey(pairs,Production_zone,Consumption_zone,MinPath)
bad1 <- merge(pairs,ineligible,by=c("Production_zone","Consumption_zone","MinPath"))
model$bad_modes <- file.path(model$outputdir,"bad_modes.csv")
if(file.exists(model$bad_modes))  {file.remove(model$bad_modes)}
if(nrow(bad1)>0)  {write.csv(bad1,file=model$bad_modes,row.names=FALSE)}


## ---------------------------------------------------------------

recycle_check_file_path <- file.path(model$outputdir,"recycle_check_final.txt")
file.create(recycle_check_file_path) #will create or truncate

## ---------------------------------------------------------------
## add code for shipments b/n zone and port (create input file first)
loadPackage("plyr")
pair1 <- pairs[MinPath<51]
pair2 <- pairs[MinPath>=51]		## international shipping
load(file.path(model$basedir,"rFreight/data/data_modepath_ports.rda"))
ports <- as.data.table(data_modepath_ports)
setkey(ports,Production_zone,Consumption_zone)
setkey(pair2,Production_zone,Consumption_zone)
pair2 <- merge(pair2,ports,by=c("Production_zone","Consumption_zone"),all.X=T)
pair2[,IntlPath:=MinPath]
pair2[,IntlDone:=0]
## -- Heither, 03-06-2017: adjust port for bulk/nonbulk goods
load(file.path(model$basedir,"rFreight/data/corresp_sctg_category.rda"))
sctgCat <- as.data.table(corresp_sctg_category)
sctgCat <- sctgCat[,list(Commodity_SCTG,Category)]
setkey(sctgCat,Commodity_SCTG)
setkey(pair2,Commodity_SCTG)
pair2 <- sctgCat[pair2]
#pair2[, Port_mesozone:=ifelse(substr(Category,1,4)=="Bulk", Port_mesozoneB, Port_mesozoneNB)]
#pair2[, Port_name:=ifelse(substr(Category,1,4)=="Bulk", as.character(Port_NameB), as.character(Port_NameNB))]
#pair2[,c("Port_mesozoneB","Port_mesozoneNB","Port_NameB","Port_NameNB"):=NULL]

## -- Imports
pair2[Production_zone>273 & Consumption_zone<=273 & IntlDone==0,Active:=1]
pair2[Active==1,ShipZone:=Production_zone]
pair2[Active==1,Production_zone:=Port_mesozone]
pair2[Active==1,Intl_zone:="P"]
pair2[Active==1,IntlDone:=1]
pair2[,Active:=NULL]
## -- Exports
pair2[Production_zone<=273 & Consumption_zone>273 & IntlDone==0,Active:=1]
pair2[Active==1,ShipZone:=Consumption_zone]
pair2[Active==1,Consumption_zone:=Port_mesozone]
pair2[Active==1,Intl_zone:="C"]
pair2[Active==1,IntlDone:=1]
pair2[,Active:=NULL]
## -- Alaska/Hawaii to Continental US
pair2[Production_zone %in% c(154,179,180) & Consumption_zone<=273 & IntlDone==0,Active:=1]
pair2[Active==1,ShipZone:=Production_zone]
pair2[Active==1,Production_zone:=Port_mesozone]
pair2[Active==1,Intl_zone:="P"]
pair2[Active==1,IntlDone:=1]
pair2[,Active:=NULL]
## -- Continental US to Alaska/Hawaii
pair2[Production_zone<=273 & Consumption_zone %in% c(154,179,180) & IntlDone==0,Active:=1]
pair2[Active==1,ShipZone:=Consumption_zone]
pair2[Active==1,Consumption_zone:=Port_mesozone]
pair2[Active==1,Intl_zone:="C"]
pair2[Active==1,IntlDone:=1]
pair2[,c("Active","Attribute2_ShipTime","MinPath","MinGmnql"):=NULL]
setkey(pair2,Production_zone,Consumption_zone)

### Calculate Domestic Linehaul-to-Port Mode
df_fin <- minLogisticsCost(pair2,1, "NAICS_PALCEHOLDER", recycle_check_file_path)
setnames(df_fin,c("time","path","minc"),c("Attribute2_ShipTime","MinPath","MinGmnql"))
setkey(df_fin,SellerID,BuyerID,NAICS,Commodity_SCTG)
setkey(pair2,SellerID,BuyerID,NAICS,Commodity_SCTG)
pair2 <- pair2[df_fin]
pair2[, Attribute1_UnitCost := MinGmnql / PurchaseAmountTons]
pair2[, Attribute2_ShipTime := Attribute2_ShipTime / 24] 			### Convert from hours to days

pairs <- as.data.table(rbind.fill(pair1,pair2))
rm(pair1,pair2,ports)

## ---------------------------------------------------------------
## Heither, revised 02-07-2016: This code block checks the pairs file for ineligible modes selected between zone pairs.
setkey(pairs,Production_zone,Consumption_zone,MinPath)
bad1 <- merge(pairs,ineligible,by=c("Production_zone","Consumption_zone","MinPath"))
model$bad_modes2 <- file.path(model$outputdir,"bad_modes2.csv")
if(file.exists(model$bad_modes2))  {file.remove(model$bad_modes2)}
if(nrow(bad1)>0)  {write.csv(bad1,file=model$bad_modes2,row.names=FALSE)}


#save the pairs table for inspection/analysis
save(pairs,file=file.path(model$outputdir,"pairs.Rdata"))
#write.csv(pairs,file=file.path(model$outputdir,"pairs.csv"),row.names=FALSE)

pmgout <- progressEnd(pmgout)


