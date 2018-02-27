##############################################################################################

#Title:             CMAP Agent Based Freight Forecasting Code

#Project:           CMAP Agent-based economics extension to the meso-scale freight model

#Description:       2_Procurement_Markets.R produces the costs inputs to the PMGs and also

#                   writes out the buy and sell inputs from the producers and cosumers tables

#Date:              June 30, 2014

#Author:            Resource Systems Group, Inc.

#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.

##############################################################################################

# Initialize the variables if not performing sensitivity analysis

if(!exists("B0")) B0 <- 10000
if(exists("B1")) model$scenvars$B1 <- B1
if(!exists("B2_mult")) B2_mult <- 1
if(!exists("B3_mult")) B3_mult <- 1
if(exists("B4")) model$scenvars$B4 <- B4
if(!exists("B5_mult")) B5_mult <- 1

if(exists("choice") & model$scenvars$runParameters) print(sprintf("Design %d:\n B0: %d, B1: %d, B2: %1.2f, B3: %1.2f, B4: %d, B5: %1.2f\n",get("choice",envir = .GlobalEnv), B0, model$scenvars$B1, B2_mult, B3_mult, model$scenvars$B4, B5_mult))






#-----------------------------------------------------------------------------------

#Step 2 Procurement Markets

#-----------------------------------------------------------------------------------

progressStart(pmg,3)
if(model$scenvars$runSensitivityAnalysis&!model$scenvars$runParameters){
  skims <- fread(file.path(model$skimsdir,"data_modepath_skims.csv"))
  # mesozone_gcd <- fread(file.path(model$skimsdir,pmg$inputs$mesozone_gcd))
}
if(model$scenvars$ApplicationMode){
  if(file.exists(file.path(model$inputdir,"ModeChoiceConstants.rds"))){
    modeChoiceConstants <- readRDS(file = file.path(model$inputdir,"ModeChoiceConstants.rds"))
  }
}



#------------------------------------------------------------------------------------------------------

# Process skims and create inputs and functions for applying models

#------------------------------------------------------------------------------------------------------

progressNextStep("Processing skims")



#Distribution Channel Model: develop correspondences

distchan_calcats <- data.table(CHOICE=c("0","1","2+","2+"),CHID=1:4) #correspondence between choice model alts and target categories int he distchan model

#add the FAMESCTG category to pairs for comparison with calibration targets

famesctg <- c(rep("A",3),rep("E",4),rep("K",2),rep("F",3),rep("C",7),rep("B",5),rep("J",6),"C",rep("G",3),"D",rep("I",2),"G","J",rep("H",4))

setnames(distchan_cal, c("CATEGORY", "CHOICE", "TARGET"))

#process gcd skims

setkey(mesozone_gcd, Production_zone, Consumption_zone)

mesozone_gcd[,c("Production_lon", "Production_lat", "Consumption_lon", "Consumption_lat"):=NULL]# Drop unneeded fields


#Shipment size model: create tabke of sizes

# ShipSize <- data.table(Ship_size = 1:4, weight = c(1000L, 10000L, 30000L, 150000L), k = 1)
ShipSize <- ShipSize[,.(Ship_size=1:.N,weight=meanWeight,k=1),by=.(Commodity_SCTG=SCTG)]



## ---------------------------------------------------------------

## Heither, revised 09-25-2015: CMAP pre-processing procedures create time & cost fields for all 54 mode paths for

##                              ALL Domestic and Foreign combinations.  RSG shortcut code is now turned off.



#### -- OBSOLETE: CMAP only skims, time and cost fields for 54 paths

setnames(skims, c("Origin", "Destination"), c("Production_zone", "Consumption_zone"))

#### -- setkey(skims, Production_zone, Consumption_zone)

if(!identical(nModes <- sum(grepl("time",colnames(skims))),nCost <- sum(grepl("cost",colnames(skims))))) stop(paste0("Information of time and cost available on ", min(nCost,nModes),"/",max(nCost,nModes)))

cskims <- skims[,c("Production_zone", "Consumption_zone",paste0("time", 1:nModes), paste0("cost", 1:nModes)),with = FALSE]

numrows <- nrow(cskims)

longcskims <- cbind(melt.data.table(cskims,id.vars = c("Production_zone","Consumption_zone"), measure.vars = paste0("time",1:nModes), variable.name = "timepath", value.name = "time"),melt.data.table(cskims[,paste0("cost",1:nModes), with=FALSE],measure.vars = paste0("cost",1:nModes), variable.name = "costpath", value.name = "cost"))

suppressWarnings(longcskims[,path:=as.numeric(rep(1:nModes,each=numrows))])
longcskims <- longcskims[!(is.na(time)&is.na(cost))]
longcskims[mode_description[,.(path=ModeNumber,Mode.Domestic=LinehaulMode)],Mode.Domestic:=i.Mode.Domestic, on=.(path)]




setkey(cskims, Production_zone, Consumption_zone)
setkey(longcskims, Production_zone, Consumption_zone)

rm(skims)


pc_domestic_targets <- pc_domestic_targets[,.(Commodity_SCTG=SCTG,Mode.Domestic,ODSegment,Target)]
##

## Heither, revised 01-29-2016: create a file of ineligible modes for each zone pair (QC review)

mdpaths <- c(1:nModes)

test <- cbind(melt(cskims[,c("Production_zone","Consumption_zone",paste0("time",mdpaths)),with=F], measure.vars=paste0("time",mdpaths), variable.name="timepath",

	value.name="time"), melt(cskims[,paste0("cost",mdpaths),with=F], measure.vars=paste0("cost",mdpaths), variable.name="costpath", value.name="cost"))

test[,MinPath:=substring(timepath, first=5)]

ineligible <- test[is.na(cost),list(Production_zone,Consumption_zone,MinPath)]

ineligible[,InEl:=1]

setkey(ineligible,Production_zone,Consumption_zone,MinPath)

## ---------------------------------------------------------------



#Prepare SCTG specific input file

setkey(sctg,Commodity_SCTG)

#Assign values for B2,B3,B5,a, and sdQ paramaters in logistics cost equation

sctg[,c("B2","B3","B5"):=c(model$scenvars$LowDiscRate,model$scenvars$MedDiscRate,model$scenvars$MedDiscRate,model$scenvars$MedDiscRate,model$scenvars$HighDiscRate)

     [match(Category,c("Bulk natural resource (BNR)","Animals","Intermediate processed goods (IPG)","Other","Finished goods (FG)"))]]

sctg[,a:=c(model$scenvars$LowMultiplier,model$scenvars$MediumMultiplier,model$scenvars$HighMultiplier)

     [match(Category2,c("Functional","Functional/Innovative","Innovative"))]]

sctg[,sdQ:=c(model$scenvars$LowVariability,model$scenvars$MediumVariability,model$scenvars$HighVariability)

     [match(Category2,c("Functional","Functional/Innovative","Innovative"))]]



#For each of the 54 possible paths, define the B0 (logistics cost equation constant) and the ls (log savings) value

#These vary depending on the commodity and mode; extremely high values of B0 are set to indicate a non-available mode-path

sctg[,paste0("B0",1:nModes):=B0]

sctg[,paste0("ls",1:nModes):=1]



#Category='Bulk natural resource (BNR)'

sctg[Category=="Bulk natural resource (BNR)",paste0("B0",1:2) := 0]

sctg[Category=="Bulk natural resource (BNR)",paste0("ls",1:2) := 0.5]

sctg[Category=="Bulk natural resource (BNR)",paste0("B0",3:12) := sctg[Category=="Bulk natural resource (BNR)",paste0("B0",3:12), with = FALSE] * 0.1]

sctg[Category=="Bulk natural resource (BNR)",paste0("B0",13:45) := sctg[Category=="Bulk natural resource (BNR)",paste0("B0",13:45), with = FALSE] * 10.0]

sctg[Category=="Bulk natural resource (BNR)",ls3 := 0.5]

### Set the pipeline (55,56,57) for 16:19 SCTG

sctg[Commodity_SCTG %in% c(16:19), paste0("B0",55:57) := 0]
sctg[Commodity_SCTG %in% c(16:19), paste0("ls",55:57) := 0.5]



#Category='Animals'

sctg[Category=="Animals",paste0("B0",3:12) := sctg[Category=="Animals",paste0("B0",3:12), with = FALSE] * 0.75]

sctg[Category=="Animals",paste0("B0",c(1:2,13:30)) := sctg[Category=="Animals",paste0("B0",c(1:2,13:30)), with = FALSE] * 10.0]

sctg[Category=="Animals",B031 := B031*0.25]



#Category='Intermediate processed goods (IPG)'

sctg[Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",1:12) := sctg[Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",1:12), with = FALSE]* 2.0]

sctg[Category=="Finished goods (FG)",paste0("B0",1:12) := sctg[Category=="Finished goods (FG)",paste0("B0",1:12), with = FALSE] * 10.0]

sctg[Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",14:30) := sctg[Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",14:30), with = FALSE] * 0.9]

sctg[Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",31:46) := sctg[Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",31:46), with = FALSE] * 0.5]



#Category='Finished goods (FG)'

sctg[Category=="Finished goods (FG)",paste0("B0",31:46) := sctg[Category=="Finished goods (FG)",paste0("B0",31:46), with = FALSE] * 0.9]

sctg[Category=="Finished goods (FG)",paste0("B0",c(32:45,47:50)) := sctg[Category=="Finished goods (FG)",paste0("B0",c(32:45,47:50)), with = FALSE] * 0.25]


# Change the mode descriptions

c_path_mode <- mode_description[,.(path=ModeNumber,Mode.Domestic=Mode)]
c_path_mode[Mode.Domestic=="Multiple",Mode.Domestic:="Rail"]
if(exists("modeChoiceConstants")) modeChoiceConstants <- modeChoiceConstants[c_path_mode,on="Mode.Domestic",allow.cartesian=TRUE]

mode_availability <- melt.data.table(mode_availability,id.vars = c("ODSegment","Commodity_SCTG","Commodity_SCTG_desc_short"),variable.factor = FALSE,value.name = "avail")[,c("path","LinehaulMode"):=tstrsplit(variable,"_")][,variable:=NULL][,path:=as.numeric(path)]
setkey(mode_availability,ODSegment,Commodity_SCTG,path)


## ---------------------------------------------------------------

## Heither, revised 07-24-2015: added model$scenvars variables, corrected B3 portion of function to include j, corrected

##                              final line of equation to implement CS code

#Define the logistics cost function used in the mode path model

calcLogisticsCost <- function(dfspi,s,path){
  set.seed(151)
  setnames(dfspi,c("pounds","weight","value","lssbd","time","cost"))

  ## variables from model$scenvars

  B1   <- model$scenvars$B1		#Constant unit per order

  B4   <- model$scenvars$B4		#Storage costs per unit per year

  j    <- model$scenvars$j		#Fraction of shipment that is lost/damaged

  sdLT <- model$scenvars$sdLT	#Standard deviation in lead time

  LT_OrderTime <- model$scenvars$LT_OrderTime	#Expected lead time for order fulfillment (to be added to in-transit time)

  #vars from S

  B2 <- s$B2*B2_mult

  B3 <- s$B3*B3_mult

  B5 <- s$B5*B5_mult

  a  <- s$a

  sdQ<- s$sdQ

  B0 <- s[[paste0("B0",path)]]

  ls <- s[[paste0("ls",path)]]



  #adjust factors for B0 and ls based on small buyer,

  if(s$Category=="Bulk natural resource (BNR)" & path %in% 5:12) B0 <- B0 - 0.25*B0*dfspi$lssbd

  if(s$Category=="Finished goods (FG)" & path %in% c(14:30,32:38)) ls <- ls - 0.25*ls*dfspi$lssbd

  if(s$Category=="Finished goods (FG)" & path %in% 39:45) ls <- ls - 0.5*ls*dfspi$lssbd



  #Calculate annual transport & logistics cost



  # NEW EQUATION



  dfspi[,minc:= B0 * runif(length(pounds)) + #changed scale of B0 -- too large for small shipment flows

          B1*pounds/weight +

          ls*(pounds/2000)*cost + #since cost is per ton

          B2*j*value +

          B3*time/24*value/365 +

          (B4 + B5*value/(pounds/2000))*weight/(2*2000) +

          (B5*(value/(pounds/2000))*a)*sqrt((LT_OrderTime+time/24)*((sdQ*pounds/2000)^2) + (pounds/2000)^2*(sdLT*LT_OrderTime)^2)]



  # OLD EQUATION



  # dfspi[,minc:= B0/1000 * runif(length(pounds)) + #changed scale of B0 -- too large for small shipment flows

  #         B1*pounds/weight +

  #         ls*(pounds/2000)*cost + #since cost is per ton

  #         B2*j*value +

  #         B3*time/24*j*value/365 +

  #         (B4 + B5*value/(pounds/2000))*weight/(2*2000) +

  #         a*sqrt((LT_OrderTime+time/24)*((sdQ*pounds/2000)^2) + (pounds/2000)^2*(sdLT*LT_OrderTime)^2)]



  return(dfspi$minc)

}

## ---------------------------------------------------------------



## ---------------------------------------------------------------

## Heither, revised 07-24-2015: added model$scenvars variables, corrected mode exclusion logic, updated Mesozone references

## Heither, revised 11-06-2015: revised mode exclusion logic due to new indirect truck modes for non-CMAP US shipments

## Heither, revised 11-24-2015: revised to include recycling check file

## Heither, revised 02-05-2016: revised so correct modepath is reported for Supplier-Buyer pair [keep NAICS/Commodity_SCTG, return as stand-alone data.table]

minLogisticsCostSctgPaths <- function(dfsp,iSCTG,paths, naics, modeChoiceConstants=NULL, recycle_check_file_path){
  # browser()


  callIdentifier <- paste0("minLogisticsCostSctgPaths(iSCTG=",iSCTG, ", paths=", paste0(collapse=", ", paths), " naics=", naics, ")")

  startTime <- Sys.time()

  # print(paste0(startTime, " Entering: ", callIdentifier, " nrow(dfsp)=", nrow(dfsp)))

  s <- sctg[iSCTG]

  dfsp <- merge(dfsp,longcskims[path %in% paths],by=c("Production_zone","Consumption_zone"),all.x=TRUE,allow.cartesian=TRUE)



  ## variables from model$scenvars

  CAP1Carload  <- model$scenvars$CAP1Carload    #Capacity of 1 Full Railcar

  CAP1FTL      <- model$scenvars$CAP1FTL		#Capacity of 1 Full Truckload

  CAP1Airplane <- model$scenvars$CAP1Airplane	#Capacity of 1 Airplane Cargo Hold



  # dfsp[,avail:=TRUE]
  dfsp[mode_availability,avail:=i.avail,on=c("ODSegment","Commodity_SCTG","path")]


  dfsp[path %in% 1:12 & weight< 0.5 * CAP1Carload,avail:=FALSE] #Eliminate Water and Carload from choice set if Shipment Size < 0.5 * Rail Carload

  dfsp[path %in% c(14,19:26,31) & weight<CAP1FTL,avail:=FALSE] #Eliminate FTL and FTL-IMX combinations from choice set if Shipment Size < 1 FTL

  dfsp[path %in% c(32:38) & weight<CAP1FTL,avail:=FALSE] #Eliminate FTL Linehaul with FTL external drayage choice set if Shipment Size < 1 FTL (Heither, 11-06-2015)

  dfsp[path %in% c(15:18,27:30,39:46) & weight>CAP1FTL,avail:=FALSE] #Eliminate LTL and its permutations from choice set if Shipment Size > FTL (Heither, 11-06-2015)

  dfsp[path %in% 47:50 & weight>CAP1Airplane,avail:=FALSE] #Eliminate Air from choice set if Shipment Size > Air Cargo Capacity

  dfsp[path %in% 51:52 & weight<(0.75*CAP1FTL),avail:=FALSE] #Eliminate Container-Direct from choice set if Shipment Size < 1 40' Container

  dfsp[path %in% 53:54 & weight<CAP1FTL,avail:=FALSE] #Eliminate International Transload-Direct from choice set if Shipment Size < 1 FTL

  dfsp[!(Commodity_SCTG %in% c(16:19)) & (path %in% c(55:57)), avail:=FALSE]

  dfsp[avail==TRUE,minc:=calcLogisticsCost(.SD[,list(PurchaseAmountTons,weight,ConVal,lssbd,time,cost)],s,unique(path)),by=path] # Faster implementation of above code

  dfsp <- dfsp[avail==TRUE & !is.na(minc)]		## limit to only viable choices before finding minimum path

  # Convert the logistics costs to a probability, adjust using constants
  # iter <- 1 # just once through if in application
    # Add the constant to the mode choice table
  if(!is.null(modeChoiceConstants)){
    dfsp[modeChoiceConstants,c("Constant","Mode.Domestic"):=.(i.Constant,i.Mode.Domestic), on=c("Commodity_SCTG","ODSegment","path")]
    dfsp <- dfsp[dfsp[,.I[which.min(minc)],.(BuyerID,SellerID,Mode.Domestic)][,V1]]
    dfsp[,c("Mode.Domestic"):=NULL]
    dfsp[Production_zone > 273 | Consumption_zone > 273, Constant:= 0]
  } else {
    dfsp[,Constant:=0]
  }

    # Calculate Probability
  dfsp[, Prob:= exp(-(minc/(10**floor(log10(max(minc))))) + Constant) / sum(exp(-(minc/(10**floor(log10(max(minc))))) + Constant)), by = .(BuyerID, SellerID)]
  dfsp[is.nan(Prob),Prob:=1]
  dfsp[is.na(Prob),Prob:=1]

    #
  dfsp[,MinCost:=0L]
  dfsp[dfsp[,.I[which.max(Prob)],by=list(SellerID,BuyerID)][,V1],MinCost:=1]

  dfsp <- dfsp[MinCost==1]

  # dfsp <- dfsp[dfsp[,.I[which.min(minc)],by=list(SellerID,BuyerID)][,V1],]
  #
  dfsp <- dfsp[,list(SellerID,BuyerID,NAICS,Commodity_SCTG,time,path,minc,weight)]



  ###if(file.exists("E:/cmh/mode_check0.txt")){

		###write.table(dfsp, file="E:/cmh/mode_check0.txt", row.names=F, col.names=F, append=TRUE, sep=",")

		###} else {write.table(dfsp, file="E:/cmh/mode_check0.txt", row.names=F, sep=",")}

  numrows2 <- nrow(dfsp)



  x_write=c(naics,iSCTG,numrows,numrows2,length(paths))

  write(x_write, recycle_check_file_path, ncolumns =length(x_write), append=TRUE)

  endTime <- Sys.time()

  # print(paste0(endTime, " Exiting after elapsed time ", format(endTime-startTime), ", ", callIdentifier, " nrow(dfsp)=", nrow(dfsp)))

  return(dfsp)

} #minLogisticsCostSctgPaths



## Heither, revised 02-05-2016: revised so correct modepath is reported for Supplier-Buyer pair [return stand-alone data.table]

minLogisticsCost <- function(df,runmode, naics, modeChoiceConstants=NULL, recycle_check_file_path){
# browser()


  pass <- 0			### counter for number of passes through function

  for (iSCTG in unique(df$Commodity_SCTG)){

    # print(paste(Sys.time(), "iSCTG: ",iSCTG))



	###### Heither, 02-09-2016: Runmode==0 - use for initial cost file development and shipper select best mode --

	if(runmode==0)	{

		#Direct (Limited to US Domestic) and anything within CMAP region (even if flagged as indirect): 4 direct mode-paths in the direct path choiceset: c(3,13,31,46)

		### -- Heither, 10-15-2015: Selection Logic simplified due to enforcing Distribution channel logical consistency above -- ###

		###df[(distchannel==1 |(Production_zone<151 & Consumption_zone<151)) & Commodity_SCTG==iSCTG,c("MinGmnql","MinPath","Attribute2_ShipTime"):=minLogisticsCostSctgPaths(df[(distchannel==1 |(Production_zone<151 & Consumption_zone<151)) & Commodity_SCTG==iSCTG],iSCTG,c(3,13,31,46), naics, recycle_check_file_path)]

		### Heither, 02-05-2016: send subset of data to function - Direct (Limited to US Domestic) and anything within CMAP region


	  df1 <- df[(distchannel==1 |(Production_zone<151 & Consumption_zone<151)) & Commodity_SCTG==iSCTG]
	  if(!is.null(modeChoiceConstants)){
	    df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(3,13,31,46,55:57), naics, modeChoiceConstants = modeChoiceConstants[ShipmentType=="D"], recycle_check_file_path)
	  } else {
	    df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(3,13,31,46,55:57), naics, modeChoiceConstants=NULL, recycle_check_file_path)
	  }

	  if(nrow(df2)>0) {if(pass==0) {

	    df_out <- copy(df2)			### make an actual copy, not just a reference to df2

	    pass <- pass + 1

	  } else {

	    df_out <- rbind(df_out,df2)

	  }

	  }



		#Indirect and International: 50 indirect mode-paths in the path choiceset: c(1:2,4:12,14:30,32:45,47:54)

		###df[distchannel>1 & (Production_zone>150 | Consumption_zone>150) & Commodity_SCTG==iSCTG,c("MinGmnql","MinPath","Attribute2_ShipTime"):=minLogisticsCostSctgPaths(df[distchannel>1 & (Production_zone>150 | Consumption_zone>150) & Commodity_SCTG==iSCTG],iSCTG,c(1:2,4:12,14:30,32:45,47:54), naics, recycle_check_file_path)]

		### Heither, 02-05-2016: send subset of data to function - Indirect and International

		df1 <- df[distchannel>1 & (Production_zone>150 | Consumption_zone>150) & Commodity_SCTG==iSCTG]

		if(!is.null(modeChoiceConstants)){
		  df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(1:2,4:12,14:30,32:45,47:54,55:57),naics, modeChoiceConstants=modeChoiceConstants[ShipmentType=="ID"], recycle_check_file_path)
		} else {
		  df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(1:2,4:12,14:30,32:45,47:54,55:57),naics, modeChoiceConstants=NULL, recycle_check_file_path)
		}

		if(nrow(df2)>0) {if(pass==0) {

			df_out <- copy(df2)

			pass <- pass + 1

			} else {

			df_out <- rbind(df_out,df2)

			}

		}

	} else {

		###### Heither, 02-09-2016: Runmode!=0 - use for shipments between domestic zone and domestic port --

		df1 <- df[Commodity_SCTG==iSCTG]
		if(is.null(modeChoiceConstants)){
		  df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(1:2,4:12,14:30,32:45,55:57), naics,modeChoiceConstants = NULL, recycle_check_file_path)			## include inland water - 03-06-2017
		} else {
		  df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(1:2,4:12,14:30,32:45,55:57), naics,modeChoiceConstants = modeChoiceConstants, recycle_check_file_path)
		}
		if(nrow(df2)>0) {if(pass==0) {

			df_out <- copy(df2)

			pass <- pass + 1

			} else {

			df_out <- rbind(df_out,df2)

			}

		}

	}

   }

	##### Heither, 02-05-2016: Following line for debugging only

	#####write.table(df_out, file="E:/cmh/mode_check1.txt", row.names=F, sep=",")

    return(df_out)

}


create_pmg_sample_groups <- function(naics,groups,sprod){

  # sort by sizes
  setkey(consc, Size)
  setkey(prodc, Size)
  #add group id and number of groups to consc and prodc; if not splitting producers assign 0
  consc[,numgroups:=groups]
  prodc[,numgroups:=groups]
  suppressWarnings(consc[,group:=1:groups])
  if(sprod==1){
    suppressWarnings(prodc[,group:=1:groups])
  } else {
    prodc[,group:=0]
  }
  #Check that for all groups the capacity > demand
  #to much demand overall?
  prodconsratio <- sum(prodc$OutputCapacityTons)/sum(consc$PurchaseAmountTons)
  if(prodconsratio < 1.1){ #TODO need to move this to variables
    #reduce consumption to ratio is >=1.1
    consc[,PurchaseAmountTons:=PurchaseAmountTons/1.1*prodconsratio]
  }
  #to much in just some groups - shuffle consumers to even out
  if(sprod==1){
    prodconsgroup <- merge(prodc[,list(OutputCapacityTons=sum(OutputCapacityTons),Producers=.N),by=group],
                           consc[,list(PurchaseAmountTons=sum(PurchaseAmountTons),Consumers=.N),by=group],by="group")
    prodconsgroup[,prodconsratio:=OutputCapacityTons/PurchaseAmountTons]
    prodconsgroup[,consexcess:=PurchaseAmountTons - OutputCapacityTons]

    iter <- 1 #counter to break in case something goes wrong and we get into an endless loop

    while (nrow(prodconsgroup[prodconsratio<1]) > 0){
      mingroup <- prodconsgroup[which.min(prodconsratio)]$group
      maxgroup <- prodconsgroup[which.max(prodconsratio)]$group
      maxgroupprod <- prodconsgroup[Producers>1][which.max(prodconsratio)]$group
      reqtomove <- prodconsgroup[mingroup]$consexcess
      maxsample <- nrow(consc[group==mingroup]) - 1 #leave at least one consumer in the group
      if (maxsample > 0){ #move consumers to other groups
        print(paste("Moving Consumers:",mingroup,"to",maxgroup,reqtomove, maxsample))
        #create a sample frame of the first maxsample records and identify a set that is just over reqtomove
        sampsellers <- sample.int(maxsample)
        constomove <- consc[group==mingroup][sampsellers,list(BuyerID,PurchaseAmountTons)]
        constomove[,PATCum:=cumsum(PurchaseAmountTons)]
        threshold <- sum(constomove$PurchaseAmountTons)-reqtomove
        consc[BuyerID %in% constomove[PATCum>threshold]$BuyerID,group:=maxgroup]
      } else { #no consumers left to move from this group so move some producers to it -- opposite direction
        maxsampleprod <- nrow(prodc[group==maxgroupprod]) - 1
        print(paste("Moving Producers:", maxgroupprod,"to",mingroup,reqtomove, maxsampleprod))
        #create a sample frame of the first maxsample records and identify a set that is just over reqtomove
        sampbuyers <- sample.int(maxsampleprod)
        prodstomove <- prodc[group==maxgroupprod][sampbuyers,list(SellerID,OutputCapacityTons)]
        prodstomove[,OCTCum:=cumsum(OutputCapacityTons)]
        threshold <- sum(prodstomove$OutputCapacityTons)-reqtomove
        prodc[SellerID %in% prodstomove[OCTCum>threshold]$SellerID,group:=mingroup]
      }
      prodconsgroup <- merge(prodc[,list(OutputCapacityTons=sum(OutputCapacityTons),Producers=.N),by=group],
                             consc[,list(PurchaseAmountTons=sum(PurchaseAmountTons),Consumers=.N),by=group],by="group")
      prodconsgroup[,prodconsratio:=OutputCapacityTons/PurchaseAmountTons]
      prodconsgroup[,consexcess:=PurchaseAmountTons - OutputCapacityTons]

      iter <- iter + 1
      if(iter==100){#break out of the loop. This should never be necessary but here to stop endless loops. Groups with excess consumption requirements will potentially run slowly
        break
      }
    }
  }
  #for casese where the producers are not being split allow consumers of buy all from one producer
  if(sprod==0) consc[,SingleSourceMaxFraction:=1.0]

}


pc_sim_distchannel <- function(pc, distchannel_food, distchannel_mfg, calibration = NULL){
  # Update progress log


  ### Create variables used in the distribution channel model
  # Create a new table so the preceding table isn't modified
  pcFlows <- copy(pc)

  # Create employment and industry dummy variables
  pcFlows[, c("emple49", "emp50t199", "empge200", "mfgind", "trwind", "whind") := 0L]
  pcFlows[Buyer.Size <= 49, emple49 := 1]
  pcFlows[Buyer.Size >= 50 & Buyer.Size <= 199, emp50t199 := 1]
  pcFlows[Buyer.Size >= 200, empge200 := 1]

  pcFlows[,Seller.NAICS2:=substr(Seller.NAICS,1,2)]
  pcFlows[Seller.NAICS2 %in% 31:33, mfgind := 1]
  pcFlows[Seller.NAICS2 %in% 48:49, trwind := 1]
  pcFlows[Seller.NAICS2 %in% c(42, 48, 49), whind := 1]

  pcFlows[,Buyer.NAICS2:=substr(Buyer.NAICS,1,2)]
  pcFlows[Buyer.NAICS2 %in% 31:33, mfgind := 1]
  pcFlows[Buyer.NAICS2 %in% 48:49, trwind := 1]
  pcFlows[Buyer.NAICS2 %in% c(42, 48, 49), whind := 1]


  # Add the FAME SCTG category for comparison with calibration targets
  pcFlows[, CATEGORY := famesctg[Commodity_SCTG]]
  setkey(pcFlows, Production_zone, Consumption_zone)

  # Add zone to zone distances
  pcFlows <- merge(pcFlows, mesozone_gcd, c("Production_zone", "Consumption_zone")) # append distances
  setnames(pcFlows, "GCD", "Distance")

  # Update progress log
  # Missing for now


  print(paste(Sys.time(), "Applying distribution channel model"))

  #Apply choice model of distribution channel and iteratively adjust the ascs

  #The model estimated for mfg products was applied to all other SCTG commodities


  inNumber <- nrow(pc[Commodity_SCTG %in% c(1:9)])

  if (inNumber > 0) {

    # Sort on vars so simulated choice is ordered correctly
    model_vars_food <- c("CATEGORY", distchannel_food[TYPE == "Variable", unique(VAR)])
    model_ascs_food <- distchannel_food[TYPE == "Constant", unique(VAR)]
    setkeyv(pcFlows, model_vars_food) #sorted on vars, calibration coefficients, so simulated choice is ordered correctly

    pcFlows_food <- pcFlows[Commodity_SCTG %in% c(1:9),model_vars_food,with=FALSE]
    pcFlows_food_weight <- pcFlows[Commodity_SCTG %in% 1:9,PurchaseAmountTons]

    df <- pcFlows_food[, list(Start = min(.I), Fin = max(.I)), by = model_vars_food] #unique combinations of model coefficients

    df[, (model_ascs_food) := 1] #add 1s for constants to each group in df

    print(paste(Sys.time(), nrow(df), "unique combinations"))

    if(!is.null(calibration)){

      pcFlows[Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchannel_food, cal = distchan_cal, calcats = distchan_calcats, weight = pcFlows_food_weight, path = file.path(model$inputdir,paste0(naics,"_g",g,"_model_distchannel_food_cal.csv")), iter = 4)]

    } else {

      pcFlows[Commodity_SCTG %in% 1:9, distchannel := predict_logit(df, distchannel_food,cal=distchan_cal,calcats=distchan_calcats)]

    }

  }

  # Update progress log


  print(paste(Sys.time(), "Finished ", inNumber, " for Commodity_SCTG %in% c(1:9)"))

  ### Apply choice model of distribution channel for other industries

  # The model estimated for mfg products is applied to all other SCTG commoditie

  outNumber <- nrow(pcFlows[!Commodity_SCTG %in% c(1:9)])

  if (outNumber > 0) {

    # Sort on vars so simulated choice is ordered correctly
    model_vars_mfg <- c("CATEGORY", distchannel_mfg[TYPE == "Variable", unique(VAR)])
    model_ascs_mfg <- distchannel_mfg[TYPE == "Constant", unique(VAR)]

    setkeyv(pcFlows, model_vars_mfg) #sorted on vars so simulated choice is ordered correctly

    pcFlows_mfg <- pcFlows[!Commodity_SCTG %in% c(1:9),model_vars_mfg,with=FALSE]
    pcFlows_mfg_weight <- pcFlows[!Commodity_SCTG %in% c(1:9),PurchaseAmountTons]

    df <- pcFlows_mfg[, list(Start = min(.I), Fin = max(.I)), by = model_vars_mfg] #unique combinations of model coefficients

    ####do this in the function (seems unecessary here)?

    df[, (model_ascs_mfg) := 1] #add 1s for constants to each group in df

    print(paste(Sys.time(), nrow(df), "unique combinations"))

    # Simulate choice -- with calibration if calibration targets provided
    if(!is.null(calibration)){

      pcFlows[!Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchannel_mfg, cal = distchan_cal, calcats = distchan_calcats, weight = pcFlows_mfg_weight, path = file.path(model$inputdir,paste0(naics,"_g",g,"_model_distchannel_mfg_cal.csv")), iter=4)]

    } else {

      pcFlows[!Commodity_SCTG %in% 1:9, distchannel := predict_logit(df, distchannel_mfg,cal = distchan_cal,calcats = distchan_calcats)]

    }

  }

  rm(df)
  print(paste(Sys.time(), "Finished ", outNumber, " for !Commodity_SCTG %in% c(1:9)"))


  # Update progress log

  return(pcFlows)


}





predict_logit <- function(df,mod,cal=NULL,calcats=NULL,weight=NULL,path=NULL,iter=1){


  #prepare the data items used in the model application and calibration
  alts <- max(mod$CHID)
  ut<-diag(alts)
  ut[upper.tri(ut)] <- 1

  if(is.numeric(df$CATEGORY)) df[,CATEGORY:=paste0("x",CATEGORY)] #numeric causes problems with column names

  cats <- unique(df$CATEGORY)
  if(!"CATEGORY" %in% names(mod)) mod<-data.table(expand.grid.df(mod,data.frame(CATEGORY=cats)))

  # if(is.numeric(cal$CATEGORY)) cal[,CATEGORY:=paste0("x",CATEGORY)]

  if(iter > 1){
    if(is.numeric(cal$CATEGORY)) cal[,CATEGORY:=paste0("x",CATEGORY)]
    if(!is.null(weight)) weightcats <- data.table(weight,cat=unlist(lapply(cats,function(x) rep(x,max(df$Fin[df$CATEGORY==x])-min(df$Start[df$CATEGORY==x])+1))))
    modcoeffs <- list()
  }

  for (iters in 1:iter){
    #calibration loops
    if(iters>1 & !is.null(cal) & !is.null(calcats)){

    #After first iternaton compare results with targets and calculate adjustment
      if(is.null(weight)){ # do count of choices
        sim <- sapply(cats,function(x) tabulate(simchoice[min(df$Start[df$CATEGORY==x]):max(df$Fin[df$CATEGORY==x])],nbins=alts))

        sim <- prop.table(sim, margin = 2)
        dimnames(sim) <- list(NULL, cats)
      } else { # do weighted sum of choices
        sim <- dcast.data.table(data.table(weightcats,simchoice), simchoice~cat, fun=sum, value.var="weight")
        sim <- merge(data.table(simchoice = 1:alts), sim, by = "simchoice", all.x = TRUE)
        sim[is.na(sim)] <- 0
        sim <- as.matrix(sim[, cats, with = FALSE])
        sim <- as.data.table(prop.table(sim, margin = 2))
      }


      if(length(unique(calcats$CHOICE))<length(unique(calcats$CHID))) { # the sim choices need to be aggregated to the calibration data
        sim <- cbind(calcats,sim)
        sim <- melt(sim,id.vars=c("CHOICE","CHID"),variable.name="CATEGORY")
        sim <- sim[,list(MODEL=sum(value)),by=list(CHOICE,CATEGORY)]
        sim <- merge(sim,cal,c("CATEGORY","CHOICE"))
        sim[MODEL==0,MODEL:=0.001] # Stop dividing it with 0
        sim[,ascadj:=log(TARGET/MODEL)]
        adj <- merge(sim,calcats,"CHOICE",allow.cartesian=TRUE)[,list(CATEGORY,CHID,ascadj)]
      }

      if(length(unique(calcats$CHOICE))>length(unique(calcats$CHID))) { # the calibratin data need to be aggregated to the sim choices
        caldat <- merge(cal[CATEGORY %in% cats],calcats,"CHOICE")
        caldat <- caldat[,list(TARGET=sum(TARGET)),by=list(CATEGORY,CHID)]
        sim <- data.table(CHID=1:nrow(sim),sim)
        sim <- melt(sim,id.vars=c("CHID"),variable.name="CATEGORY")
        sim <- merge(sim,caldat,c("CATEGORY","CHID"))
        sim[value==0,value:=0.001] # Stop dividing it with 0
        sim[,ascadj:=log(TARGET/value)]
        adj <- sim[,list(CATEGORY,CHID,ascadj)]

      }

      if(length(unique(calcats$CHOICE))==length(unique(calcats$CHID))) {
        stop("Need to implment calibration for same calcats as choice alts")
        #####TODO add in some code here for the other case:
        #####1. same number of choices as calibration categories (so not aggregation required)
      }

      mod <- merge(mod,adj,c("CATEGORY","CHID"))
      mod[TYPE=="Constant",COEFF:=COEFF+ascadj]
      mod[,ascadj:=NULL]
    }

    #apply the choice model

    ### -- Heither, 08-06-2015: the following line was modified to use the pmin function to cap values at 600 before using the exponential function to prevent INF values --##

    ##utils <- lapply(cats, function(y) sapply(1:alts, function(x) exp(rowSums(sweep(df[CATEGORY==y,mod[CHID==x & CATEGORY==y,VAR],with=F],2,mod[CHID==x & CATEGORY==y,COEFF],"*")))))

    utils <- lapply(cats, function(y) sapply(1:alts, function(x) exp(pmin(rowSums(sweep(df[CATEGORY==y,mod[CHID==x & CATEGORY==y,VAR],with=F],2,mod[CHID==x & CATEGORY==y,COEFF],"*")),600))))
    utils <- lapply(1:length(cats),function(x) if(is.null(dim(utils[[x]]))){(utils[[x]]/sum(utils[[x]])) %*% ut} else {(utils[[x]]/rowSums(utils[[x]])) %*% ut})
    utils <- do.call("rbind",utils)
    set.seed(151)
    temprand <- runif(max(df$Fin))
    simchoice <- unlist(lapply(1:nrow(df),function(x) 1L + findInterval(temprand[df$Start[x]:df$Fin[x]],utils[x,])))

    # for calibration
    if(iter>1) modcoeffs[[length(modcoeffs)+1]] <- mod
  }
  if(iter > 1) {
    fwrite(modcoeffs[[length(modcoeffs)]][,weight:=sum(weight)], file=path)
  }
  return(simchoice)

}




create_pmg_inputs <- function(naics,g,sprod, recycle_check_file_path){


  starttime <- proc.time()

  # Update foreign demand if domestic production cannot meet the foreign demand
  conscg <- consc[group==g, .(BuyerID, NAICS = InputCommodity, Commodity_SCTG, Zone, PurchaseAmountTons)]
  prodcg <- prodc[group==g, .(SellerID, NAICS = OutputCommodity, Commodity_SCTG, Zone, OutputCapacityTons)]

  # Calculate summaries
  extdemand <- conscg[,sum(PurchaseAmountTons),.(NAICS,Zone,Commodity_SCTG)][order(V1)][Zone>273][,.(demand = sum(V1)),.(NAICS, Commodity_SCTG)]
  extprod <- prodcg[,sum(OutputCapacityTons),.(NAICS,Zone,Commodity_SCTG)][order(V1)][Zone>273][,.(production = sum(V1)),.(NAICS, Commodity_SCTG)]
  intdemand <- conscg[,sum(PurchaseAmountTons),.(NAICS,Zone,Commodity_SCTG)][order(V1)][Zone<=273][,.(demand = sum(V1)),.(NAICS, Commodity_SCTG)]
  intprod <- prodcg[,sum(OutputCapacityTons),.(NAICS,Zone,Commodity_SCTG)][order(V1)][Zone<=273][,.(production = sum(V1)),.(NAICS, Commodity_SCTG)]

  # Flag foreign demands not met by domestic production
  intprod_to_extdemand <- merge(intprod,extdemand,by=c("NAICS","Commodity_SCTG"),all = TRUE)
  intprod_to_extdemand[is.na(demand),demand:= 0]
  intprod_to_extdemand[is.na(production),production:= 0]
  intprod_to_extdemand[demand != 0,pcratio:= production/demand]
  intprod_to_extdemand[demand == 0, pcratio := 1]
  intprod_to_extdemand <- intprod_to_extdemand[pcratio < 1]
  if(intprod_to_extdemand[,.N]>0){
	fwrite(intprod_to_extdemand, file = file.path(model$outputdir, paste0(naics, "_g", g, "_intextpcratio.csv")))
	# Update demand of foreign buyers 95% of pc ratio
	conscg_ext <- conscg[Zone>273]
	conscg_ext[intprod_to_extdemand,PurchaseAmountTons:= PurchaseAmountTons * pcratio * 0.95,on=.(NAICS,Commodity_SCTG)]
	consc[conscg_ext,PurchaseAmountTons:=i.PurchaseAmountTons,on=.(BuyerID,InputCommodity==NAICS, Commodity_SCTG)]
	rm(conscg_ext)
  }
  rm(conscg, prodcg, extdemand, extprod, intdemand, intprod, intprod_to_extdemand)
  gc()

  print(paste(Sys.time(), "Writing buy and sell files for ",naics,"group",g))




  #All consumers for this group write PMG input and create table for merging

  fwrite(consc[group==g,list(InputCommodity,BuyerID,FAFZONE,Zone,NAICS,Size,OutputCommodity,PurchaseAmountTons,PrefWeight1_UnitCost,PrefWeight2_ShipTime,SingleSourceMaxFraction)],

            file = file.path(model$outputdir,paste0(naics, "_g", g, ".buy.csv")))

  conscg <- consc[group==g,list(InputCommodity,Commodity_SCTG,NAICS,FAFZONE,Zone,Buyer.SCTG,BuyerID,Size,ConVal,PurchaseAmountTons)]



  print(paste(Sys.time(), "Finished writing buy file for ",naics,"group",g))


  ###########################
  # Group Approach
  ###########################
  #If splitting producers, write out each group and else write all with output capacity reduced
  if(sprod==1){
    # There is no grouping of producers since we are sampling

    fwrite(prodc[group==g,list(OutputCommodity,SellerID,FAFZONE,Zone,NAICS,Size,OutputCapacityTons,NonTransportUnitCost)],

           file = file.path(model$outputdir,paste0(naics, "_g", g, ".sell.csv")))

    prodcg <- prodc[group==g,list(OutputCommodity,NAICS,Commodity_SCTG,SellerID,Size,FAFZONE,Zone,OutputCapacityTons)]

  } else {

    #reduce capacity based on demand in this group

    consamount <- sum(conscg$PurchaseAmountTons)/sum(consc$PurchaseAmountTons)

    prodc[,OutputCapacityTonsG:= OutputCapacityTons * consamount]

    fwrite(prodc[,list(OutputCommodity,SellerID,FAFZONE,Zone,NAICS,Size,OutputCapacityTons=OutputCapacityTonsG,NonTransportUnitCost)],

              file = file.path(model$outputdir,paste0(naics, "_g", g, ".sell.csv")))

    prodcg <- prodc[,list(OutputCommodity,NAICS,Commodity_SCTG,SellerID,Size,FAFZONE,Zone,OutputCapacityTons=OutputCapacityTonsG)]

  }

  print(paste(Sys.time(), "Finished writing sell file for ",naics,"group",g))



  print(paste(Sys.time(), "Applying distribution, shipment, and mode-path models to",naics,"group",g))

  # Rename ready to merge

  setnames(conscg, c("InputCommodity", "NAICS", "Zone","Size"), c("NAICS", "Buyer.NAICS", "Consumption_zone","Buyer.Size"))

  setnames(prodcg, c("OutputCommodity", "NAICS", "Zone","Size"),c("NAICS", "Seller.NAICS", "Production_zone","Seller.Size"))

  longcskims_sctg <- mode_availability[avail==TRUE & Commodity_SCTG %in% unique(prodcg$Commodity_SCTG)][,.(Commodity_SCTG,ODSegment,path)][longcskims,on=.(path),allow.cartesian=TRUE][!is.na(ODSegment)]

  distance_bins <- model$scenvars$distance_bins
  nSupplierPerBuyer <- model$scenvars$nSupplierPerBuyer
  distBased <- model$scenvars$distBased

  # Bin the distances
  mesozone_gcd[,Distance_Bin:=findInterval(GCD,distance_bins)]

  if(model$scenvars$distBased){
    supplier_selection_distribution <- fread(file.path(model$basedir,"scenarios","base","inputs","DistanceDistribution.csv"),key=c("SCTG","DistanceGroup"))
  } else {
    supplier_selection_distribution <- fread(file.path(model$basedir,"scenarios","base","inputs","TonnageDistribution.csv"),key=c("SCTG","DistanceGroup"))
  }

  # Get modal availability
  zonemodeavailability <- dcast.data.table(longcskims_sctg,Commodity_SCTG+Production_zone+Consumption_zone~Mode.Domestic, value.var = "Mode.Domestic", fun.aggregate = function(x) ifelse(length(x)>0,TRUE,FALSE))

  # Get the modal cost data
  modal_targets <- pc_domestic_targets[Commodity_SCTG %in% unique(prodcg$Commodity_SCTG)]
  modal_targets <- modal_targets[mode_availability[Commodity_SCTG%in%unique(prodcg$Commodity_SCTG)&avail==TRUE,.N,.(Mode.Domestic=LinehaulMode,ODSegment,Commodity_SCTG)],on=.(Commodity_SCTG,ODSegment,Mode.Domestic)][,N:=NULL]
  modal_targets[is.na(Target), Target:= 0]

  # Get mode specific weights for zone pairs and commodity type
  modal_weights <- longcskims_sctg[,.(weights=1/min(cost)),.(Production_zone,Consumption_zone,Mode.Domestic,ODSegment,Commodity_SCTG)]

  # Merge modal targets
  modal_weights <- merge(modal_weights,modal_targets,by=c("Mode.Domestic","ODSegment","Commodity_SCTG"))


  # Get the weights according to the target
  modal_weights <- modal_weights[,.(modalweights=sum(weights*Target)),.(Production_zone,Consumption_zone,ODSegment,Commodity_SCTG)]
  modal_weights[is.na(modalweights), modalweights:=1e-6]



  # merge together to create all of the pairs of firms for this group and commodity

  set.seed(151)
  cthresh <- model$scenvars$combinationthreshold
  nSupplierPerBuyer <- model$scenvars$nSuppliersPerBuyer
  cores_used <- model$scenvars$maxcostrscripts
  pc <- data.table()
  n_splits <- 1L
  nconst <- as.numeric(conscg[,.N])
  nprodt <- as.numeric(prodcg[,.N])
  size_per_row <- 250
  ram_to_use <- 0.85*(model$scenvars$availableRAM) # Approximately 85% of available RAM
  n_splits <- ceiling(nconst*nprodt*size_per_row/(ram_to_use/(cores_used)))
  suppressWarnings(conscg[,n_split:=1:n_splits])
  # Set the initial ratioweights to 1. ratioweights attribute is used to track a metric of production to consumption ratio
  prodcg[,ratioweights:=1]

  customSample <- function(SellerID,...){
    if(length(SellerID)>1){
      return(sample(SellerID,...))
    } else {
      return(SellerID)
    }
  }

  # Make all the producers and consumers available for sample
  prodcg[,availableForSample:=TRUE]
  conscg[,doSample:=TRUE]

  prodcg[,tonquant:=findInterval(OutputCapacityTons,quantile(OutputCapacityTons,type = 5),rightmost.closed = TRUE)]
  conscg[,tonquant:=findInterval(PurchaseAmountTons,quantile(PurchaseAmountTons,type = 5),rightmost.closed = TRUE)]

  # Assign a sample size (of sellers) to buyers
  conscg[,samplesize:=seq(5,nSupplierPerBuyer,length.out=4)[tonquant]]

  # Sampling function
  sample_data <- function(split_number,fractionOfSupplierperBuyer=1.0,samplingforSeller=FALSE,pc_data=NULL,increaseRatio=FALSE){
    # browser()
    # Merge producers and consumers
    if(!increaseRatio){
      pc_split <- merge(prodcg[availableForSample==TRUE][,availableForSample:=NULL][,tonquant:=NULL],conscg[n_split==split_number & doSample==TRUE][,c("n_split","doSample","tonquant"):=NULL],by = c("NAICS","Commodity_SCTG"), allow.cartesian = TRUE,suffixes = c(".supplier",".buyer"))
    } else {
      pc_split <- merge(prodcg[availableForSample==TRUE][,availableForSample:=NULL],conscg[n_split==split_number & doSample==TRUE][,c("n_split","doSample"):=NULL], by = c("NAICS","Commodity_SCTG","tonquant"), allow.cartesian = TRUE,suffixes = c(".supplier",".buyer"))[,tonquant:=NULL]
    }


    pc_split <- pc_split[Production_zone<=273 | Consumption_zone<=273]	### -- Heither, 10-14-2015: potential foreign flows must have one end in U.S. so drop foreign-foreign

    if(!is.null(pc_data)){
      # if pc_data is passed then remove all the pairs that alread exist
      pc_split <- pc_split[!pc_data[,.(BuyerID,SellerID,Commodity_SCTG)],on=.(BuyerID,SellerID,Commodity_SCTG)]
      gc()
    }

    # Get the OD pair type
    pc_split[FAFZONE.supplier==FAFZONE.buyer,ODSegment:="I"]
    pc_split[FAFZONE.supplier!=FAFZONE.buyer,ODSegment:="X"]

    if((pc_split[,.N]>0)){

      pc_split[mesozone_gcd,Distance_Bin:=i.Distance_Bin,on=.(Production_zone,Consumption_zone)] # The distance bin that a buyer supplier falls into

      # Merge the pairs with modal weights later used for sampling
      pc_split <- merge(modal_weights[modalweights>1e-10],pc_split, by=c("Production_zone","Consumption_zone","ODSegment","Commodity_SCTG"),allow.cartesian = TRUE)

      # Get the sampling proportion from the supplier ton distribution
      pc_split[,Proportion:=supplier_selection_distribution[.(Commodity_SCTG,Distance_Bin),Proportion]] # Assign probability of selection based on the distribution
      pc_split[is.na(Proportion), Proportion:=1e-6]

      # Weight the probabilities by outputcapacity and purchasing amount. Higher ratio means higher chance of selection. More weight to sellers with outputcapacity closer to purchaseamounttons
      pc_split[,prodweights:=1/(1+0.01*log1p(pmax(0,PurchaseAmountTons-OutputCapacityTons))+4*log1p(pmax(0,OutputCapacityTons-PurchaseAmountTons)))]

      # Get the weighted proportion
      pc_split[,Proportion:=(Proportion/sum(Proportion))*(modalweights/sum(modalweights))*(prodweights/sum(prodweights))*(ratioweights/sum(ratioweights)),.(BuyerID,Commodity_SCTG)]  #*prop.table(prodweights)


      pc_split[,c("ratioweights","modalweights","prodweights"):=NULL]

      # Assign a sample size of suppliers to be picked for each buyer based on the purchaseamounttons
      pc_split[conscg[n_split==split_number],weights:=i.samplesize,on="BuyerID"] # Assign number of suppliers per buyer
      resample <- TRUE # Check to see all the sellers can meet the buyers demand.
      sampleIter <- 1
      buffer <- 0.25
      # sampled_pairs <- data.table()
      while(resample) {

        # Sample list of sellers
        if(sampleIter > 1){
          sampled_pairs <- rbindlist(list(sampled_pairs,pc_split[buyerdetail[dfTons<(buffer*PurchaseAmountTons)], on=.(BuyerID)][, .(SellerID = customSample(SellerID, min(.N, ceiling(weights*fractionOfSupplierperBuyer)), replace = FALSE, prob = Proportion/sum(Proportion))), by = .(BuyerID,Commodity_SCTG)]))
        } else {
          sampled_pairs <- pc_split[, .(SellerID = customSample(SellerID, min(.N, ceiling(weights*fractionOfSupplierperBuyer)), replace = FALSE, prob = Proportion/sum(Proportion))), by = .(BuyerID,Commodity_SCTG)]
        }

        # Check if the buyers purchasing requirement is met by the paired sellers capacity
        buyerdetail <- pc_split[sampled_pairs,on=c("BuyerID","SellerID", "Commodity_SCTG")][,.(dfTons=sum(OutputCapacityTons)),by=.(BuyerID,PurchaseAmountTons,Commodity_SCTG)]
        resample <- buyerdetail[,any(dfTons<(PurchaseAmountTons*buffer))] & pc_split[buyerdetail[dfTons<(buffer*PurchaseAmountTons)],on=.(BuyerID)][,.N,.(BuyerID,weights)][,any(weights < N)]# Make sure that the buyers demand could be fulfilled.

        # Do not resample if the sampling is done to match prod/cons ratio or modal ratio


        buyerdetail[,samplemultiplier:=1L]
        buyerdetail[dfTons<(buffer*PurchaseAmountTons),samplemultiplier:=rep(c(5L,2L),c(3,7))[findInterval(dfTons/(buffer*PurchaseAmountTons),seq(0,1,.1),rightmost.closed = TRUE)]]


        resample  <- resample&!samplingforSeller

        if(resample){
          pc_split[buyerdetail[dfTons<(buffer*PurchaseAmountTons)],weights:=weights*i.samplemultiplier,on=.(BuyerID)]
          conscg[buyerdetail[dfTons<PurchaseAmountTons],samplesize:=samplesize*i.samplemultiplier,on=.(BuyerID)]
          sampled_pairs <- sampled_pairs[!buyerdetail[dfTons <  (buffer*PurchaseAmountTons),.(BuyerID)], on=.(BuyerID)]
        }

        print(paste0("Number of sampling iterations for Sellers: ",sampleIter))
        print(paste0("Buyers Remaining: ", buyerdetail[dfTons<(buffer*PurchaseAmountTons),.N]))
        print(pc_split[buyerdetail[dfTons<(buffer*PurchaseAmountTons)],on=.(BuyerID)][,.N,.(BuyerID,weights)])
        print(sprintf("Number of sampled rows: %d", sampled_pairs[,.N]))

        # Increase the number of sampling iteration
        sampleIter <- sampleIter+1
      }
      print(paste0("Number of sampling iterations for Sellers: ",sampleIter-1))
      print(sprintf("Split Number: %d/%d",split_number,n_splits))
      print(sprintf("Number of pc_split rows: %d", pc_split[,.N]))
      # print(paste0(object.size(pc_split)/(1024**3)," Gb"))
      # print(paste0(object.size(pc_split[sampled_pairs,on=c("BuyerID","SellerID")])/(1024**3)," Gb"))
      return(pc_split[sampled_pairs,on=c("BuyerID","SellerID","Commodity_SCTG")][,c("Proportion","weights"):=NULL])
    } else {
      return(NULL)
    }
  }

  # Checking if flows can happen function
  SolveFlowGLPK <- function(pc_table,firstrun=TRUE){
    # Get the number of variables
    SellerID <- prodcg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG), SellerID]
    BuyerID <- conscg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG), BuyerID]
    nSellers <- (length(SellerID))
    nBuyers <- (length(BuyerID))
    flowlength <- pc_table[,.N]
    sellerweights <- c(1,0.75,0.5,0.25)[prodcg[pc_table,tonquant,on=.(SellerID)]]
    buyerweigths <- c(1,0.75,0.5,0.25)[conscg[pc_table,tonquant,on=.(BuyerID)]]
    objweights <- sellerweights*buyerweigths

    # The objective function to minimize is just the sum of flows, penalty on slack variables for sellers and slack variables on buyers
    ## Slack Variables, Flow Variables
    objective <- c(rep(10,nBuyers),objweights) #,rep(10,nSellers),rep(10,nBuyers))

    # The first constraint is to not oversell
    # Sellers are i and Buyers are j. Columns are flows and slack and first chunk of rows are sellers.
    # Column 1 will be i=1,j=1, Column 2 will be i=2, j=1, until Column=length(SellerID) where i=length(SellerID), j = 1

    rowindex1 <- match(pc_table$SellerID,SellerID)
    colindex1 <- 1:flowlength + nBuyers
    rhsoutput1 <- pc_table[,.N,.(SellerID)][prodcg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG)],OutputCapacityTons,on=.(SellerID)]

    slackrowindex1 <- 1:nSellers
    slackcolindex1 <- 1:nSellers + nBuyers


    rowindex2 <- match(pc_table$BuyerID,BuyerID)
    colindex2 <- 1:flowlength + nBuyers
    rhsoutput2 <- pc_table[,.N,.(BuyerID)][conscg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG)],PurchaseAmountTons,on=.(BuyerID)]
    rowindex2 <- rowindex2 + nSellers

    slackrowindex2 <- 1:nBuyers
    slackcolindex2 <- 1:nBuyers

    # constraintmatrix <- sparseMatrix(i=c(rowindex1,slackrowindex1,rowindex2,slackrowindex2), j=c(colindex1, slackcolindex1, colindex2, slackcolindex2), x = c(rep(1,length(rowindex1)+length(slackrowindex1)+length(rowindex2)),rep(-1,length(slackrowindex2))), dimnames = list(c(SellerID,BuyerID), seq_len(flowlength+nSellers+nBuyers)))

    constraintmatrix <- slam::as.simple_triplet_matrix(Matrix::sparseMatrix(i=c(rowindex1, rowindex2, slackrowindex2, slackrowindex1), j=c(colindex1, colindex2, slackcolindex2, slackcolindex1), x = 1, dimnames = list(c(SellerID,BuyerID), seq_len(flowlength + nBuyers + nSellers))))

    NAICS <- unique(pc_table$NAICS)
    Commodity_SCTG <- unique(pc_table$Commodity_SCTG)

    rm(pc_table)
    gc()

    if(firstrun){


      ## Create a new problem
      tradeproblem <- initProbGLPK()

      ## Set the direction of the objective function
      setObjDirGLPK(tradeproblem, GLP_MIN)

      ## Add the number of rows and columns
      addRowsGLPK(tradeproblem, nBuyers+nSellers)
      addColsGLPK(tradeproblem, nBuyers)
      addColsGLPK(tradeproblem, nSellers)
      addColsGLPK(tradeproblem, flowlength)

      ## Set the objective coefficients
      setObjCoefsGLPK(tradeproblem,seq_len(flowlength+nBuyers+nSellers),as.double(objective))

      ## Set the columns and rows bounds
      setColsBndsGLPK(tradeproblem,seq_len(flowlength+nBuyers+nSellers), lb = as.double(rep(0, flowlength+nBuyers+nSellers)), ub = as.double(rep(max(rhsoutput1,rhsoutput2), flowlength+nBuyers+nSellers)))
      setRowsBndsGLPK(tradeproblem,seq_len(nSellers+nBuyers),lb = as.double(c(rep(0, nSellers),rhsoutput2)), ub = as.double(c(rhsoutput1,rep(Inf, nBuyers))))

      ## Load constraint matrix
      loadMatrixGLPK(tradeproblem, as.integer(length(constraintmatrix$i)), as.integer(constraintmatrix$i), as.integer(constraintmatrix$j), as.double(constraintmatrix$v))

      ## Set the row names
      setRowsNamesGLPK(tradeproblem,as.integer(seq_len(nSellers+nBuyers+nSellers)),c(SellerID,BuyerID))
      # setColsNamesGLPK(tradeproblem,as.integer(seq(flowlength+1,flowlength+nBuyers)),paste0("S-",BuyerID))

      ## Set Simplex Control Parameters

      ## Solve the problem
      solveSimplexGLPK(tradeproblem)

      sellers <- SellerID[getRowsDualGLPK(tradeproblem)[1:nSellers]==0]
      buyersslack <- getColsPrimGLPK(tradeproblem)[1:nBuyers]
      buyers <- BuyerID[buyersslack > 0]
      ifelse(sum(buyersslack)>0, status <- 1, status <- 0)

      lp_problems[[paste0(NAICS,"-",Commodity_SCTG)]] <<- tradeproblem

      return(list(data.table(SellerID=sellers),data.table(BuyerID=buyers),status))
    } else {
      # Retrieve the original problem
      tradeproblem <- lp_problems[[paste0(NAICS,"-",Commodity_SCTG)]]

      # Retrieve the original number of columns
      num_columns <- getNumColsGLPK(tradeproblem)

      ## Add the new columns
      addColsGLPK(tradeproblem, flowlength-(num_columns-nBuyers))

      ## Set the objective coefficients
      setObjCoefsGLPK(tradeproblem,colindex1,as.double(objective))

      ## Set the columns bounds
      setColsBndsGLPK(tradeproblem, colindex1, lb = as.double(rep(0, flowlength)), ub = as.double(rep(Inf, flowlength)))

      ## Load constraint matrix
      loadMatrixGLPK(tradeproblem, as.integer(length(constraintmatrix$i)), as.integer(constraintmatrix$i), as.integer(constraintmatrix$j), as.double(constraintmatrix$v))

      ## Set the row names
      # setRowsNamesGLPK(tradeproblem,as.integer(seq_len(nSellers+nBuyers)),c(SellerID,BuyerID))
      # setColsNamesGLPK(tradeproblem,as.integer(seq(flowlength+1,flowlength+nBuyers)),paste0("S-",BuyerID))

      ## Set Simplex Control Parameters

      ## Solve the problem
      solveSimplexGLPK(tradeproblem)

      sellers <- SellerID[getRowsDualGLPK(tradeproblem)[1:nSellers]==0]
      buyersslack <- getColsPrimGLPK(tradeproblem)[1:nBuyers]
      buyers <- BuyerID[buyersslack > 0]
      ifelse(sum(buyersslack)>0, status <- 1, status <- 0)

      lp_problems[[paste0(NAICS,"-",Commodity_SCTG)]] <<- tradeproblem

      return(list(data.table(SellerID=sellers),data.table(BuyerID=buyers),status))
    }
  }

  SolveFlowCLP <- function(pc_table,firstrun=TRUE){
    # Get the number of variables
    SellerID <- prodcg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG), SellerID]
    BuyerID <- conscg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG), BuyerID]
    nSellers <- (length(SellerID))
    nBuyers <- (length(BuyerID))
    flowlength <- pc_table[,.N]
    sellerweights <- c(1,0.75,0.5,0.25)[prodcg[pc_table,tonquant,on=.(SellerID)]]
    buyerweigths <- c(1,0.75,0.5,0.25)[conscg[pc_table,tonquant,on=.(BuyerID)]]
    objweights <- sellerweights*buyerweigths

    # The objective function to minimize is just the sum of flows, penalty on slack variables for sellers and slack variables on buyers
    ## Slack Variables, Flow Variables
    objective <- c(rep(10,nBuyers),objweights) #,rep(10,nSellers),rep(10,nBuyers))

	#Problem formulation:
	# S: Set of sellers
	# B: Set of buyers
	# f_sb: flow from seller s to buyer b
	# c_sb: cost of flow from seller s to buyer b
	# s_b: unfulfilled demand of buyer b
	# D_b: Demand of buyer b
	# C_s: Production capacity of seller s
	## min sum_sb c_sb * f_sb + sum_b lambda * s_b
	## s.t.
	## 		0 <= sum_b f_sb <= C_s for all s
	##		D_b <= s_b + sum_s f_sb for all b

    # The first constraint is to not oversell
    # Sellers are i and Buyers are j. Columns are flows and slack and first chunk of rows are sellers.
    # Column 1 will be i=1,j=1, Column 2 will be i=2, j=1, until Column=length(SellerID) where i=length(SellerID), j = 1

    rowindex1 <- match(pc_table$SellerID,SellerID)
    rhsoutput1 <- pc_table[,.N,.(SellerID)][prodcg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG)],OutputCapacityTons,on=.(SellerID)]

    rowindex2 <- match(pc_table$BuyerID,BuyerID)
    rhsoutput2 <- pc_table[,.N,.(BuyerID)][conscg[NAICS %in% unique(pc_table$NAICS) & Commodity_SCTG %in% unique(pc_table$Commodity_SCTG)],PurchaseAmountTons,on=.(BuyerID)]
    rowindex2 <- rowindex2 + nSellers

    slackrowindex2 <- 1:nBuyers


    rowindex <- integer(length = length(rowindex1) + length(rowindex2) + length(slackrowindex2))
	rowindex[1:nBuyers] <- slackrowindex2 - 1
	rowindex[nBuyers + seq(1,2*flowlength,2)] <- rowindex1 - 1
	rowindex[nBuyers + seq(2,2*flowlength,2)] <- rowindex2 - 1

	colindex <- c(1:nBuyers, nBuyers + seq(1,2*flowlength,2)) - 1
	colindex <- c(colindex, length(rowindex))

	clb <- as.double(rep(0, flowlength+nBuyers))
	cub <- as.double(rep(max(rhsoutput1,rhsoutput2), flowlength+nBuyers))

	rlb <- as.double(c(rep(0, nSellers),rhsoutput2))
	rub <- as.double(c(rhsoutput1,rep(Inf, nBuyers)))

	ar <- double(length = length(rowindex)) + 1


    NAICS <- unique(pc_table$NAICS)
    Commodity_SCTG <- unique(pc_table$Commodity_SCTG)

    rm(pc_table)
    gc()

    if(firstrun){


      ## Create a new problem
      tradeproblem <- tradeproblem <- initProbCLP()

      ## Set the direction of the objective function
      setObjDirCLP(tradeproblem, 1)

      ## Load model problem
      loadProblemCLP(tradeproblem, nBuyers+flowlength, nBuyers+nSellers, ia = rowindex, ja = colindex, ra = ar, rlb = rlb, rub = rub, obj_coef = objective, lb = clb, ub = cub)

	  ## Set Simplex Control Parameters
	  setLogLevelCLP(tradeproblem, 1)

      ## Solve the problem
      solveInitialPrimalCLP(tradeproblem)

      sellers <- SellerID[getRowDualCLP(tradeproblem)[1:nSellers]==0]
      buyersslack <- getColPrimCLP(tradeproblem)[1:nBuyers]
      buyers <- BuyerID[buyersslack > 0]
      ifelse(sum(buyersslack)>0, status <- 1, status <- 0)

      lp_problems[[paste0(NAICS,"-",Commodity_SCTG)]] <<- tradeproblem

      return(list(data.table(SellerID=sellers),data.table(BuyerID=buyers),status))
    } else {
      # Retrieve the original problem
      tradeproblem <- lp_problems[[paste0(NAICS,"-",Commodity_SCTG)]]

      # Retrieve the original number of columns
      num_columns <- getNumColsCLP(tradeproblem)

	  # Number of new columns
	  num_new_columns <- flowlength-(num_columns-nBuyers)
	  clb_new <- clb[(num_columns + 1):(length(clb))]
	  cub_new <- cub[(num_columns + 1):(length(cub))]
	  obj_new <- objective[(num_columns + 1):(length(objective))]
	  colst_new <- colindex[(num_columns + 1):(length(colindex))]
	  colst_new <- colst_new - min(colst_new)
	  row_start_index <- num_columns*2 - nBuyers + 1
	  rows_new <- rowindex[(row_start_index):(length(rowindex))]
	  ar_new <- ar[(row_start_index):(length(ar))]


      ## Add the new columns
	  addColsCLP(tradeproblem, num_new_columns, clb_new, cub_new, obj_new, colst_new, rows_new, ar_new)

      ## Solve the problem
      solveInitialPrimalCLP(tradeproblem)

      sellers <- SellerID[getRowDualCLP(tradeproblem)[1:nSellers]==0]
      buyersslack <- getColPrimCLP(tradeproblem)[1:nBuyers]
      buyers <- BuyerID[buyersslack > 0]
      ifelse(sum(buyersslack)>0, status <- 1, status <- 0)

      lp_problems[[paste0(NAICS,"-",Commodity_SCTG)]] <<- tradeproblem

      return(list(data.table(SellerID=sellers),data.table(BuyerID=buyers),status))
    }
  }

  print("1. First time sampling")
  pc <- rbindlist(lapply(1:n_splits,sample_data))

  lp_problems <- list()

  solution <- pc[,.(status = SolveFlowCLP(.SD)),by=.(NAICS,Commodity_SCTG),.SDcols = c("SellerID","BuyerID","OutputCapacityTons","PurchaseAmountTons", "NAICS", "Commodity_SCTG")]

  SellersToDo <- solution[seq(1,.N,by=3),rbindlist(status),.(NAICS,Commodity_SCTG)]
  BuyersToDo <- solution[seq(2,.N,by=3),rbindlist(status),.(NAICS,Commodity_SCTG)]
  solution <- solution[seq(3,.N,by=3)]

  prodratio <- pc[,.(dfTons=sum(PurchaseAmountTons)),.(SellerID,OutputCapacityTons)][,.(SellerID, Ratio=dfTons/OutputCapacityTons)]
  # prodcg[prodratio[Ratio > 100],availableForSample:=FALSE,on=.(SellerID)]


  # Update the ratioweights
  prodcg[prodratio,ratioweights:=1/i.Ratio,on="SellerID"]
  prodcg[!prodratio,ratioweights:=100,on="SellerID"]

  conscg[doSample==TRUE,samplesize:=(samplesize+20)]

  buyeriter <- 1

  isInfeasible <- solution[,any(status>0)]
  NAICSmarketnottodo <- solution[status==0,.(NAICS,Commodity_SCTG)]

  prodcg[NAICSmarketnottodo,availableForSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
  conscg[NAICSmarketnottodo,doSample:=FALSE, on=.(NAICS,Commodity_SCTG)]

  totalPairs <- pc[,.N]

  NAICSMarkettodo <- pc[,.N,.(NAICS,Commodity_SCTG)][,N:=NULL][,status:=1]

  while(isInfeasible){
    # First do the sampling for selected sellers
    prodcg[,availableForSample:=TRUE]
    prodcg[NAICSmarketnottodo,availableForSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
    prodcg[!SellersToDo,availableForSample:=FALSE,on=.(NAICS, Commodity_SCTG, SellerID)]
    conscg[,doSample:=TRUE]
    conscg[NAICSmarketnottodo, doSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
    # conscg[!BuyersToDo, doSample:=FALSE,on=.(NAICS,Commodity_SCTG,BuyerID)]
    new_pc <- rbindlist(lapply(1:n_splits, sample_data, pc_data = pc, samplingforSeller = TRUE))

    pc <- rbindlist(list(pc,new_pc))

    if(new_pc[,.N]>0){
      NAICSMarkettodo[new_pc[,.N,.(NAICS,Commodity_SCTG)],status:=0,on=.(NAICS,Commodity_SCTG)]
    }
    # Second do the sampling for selected buyers
    prodcg[,availableForSample:=TRUE]
    prodcg[NAICSmarketnottodo,availableForSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
    conscg[,doSample:=TRUE]
    conscg[NAICSmarketnottodo, doSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
    conscg[!BuyersToDo, doSample:=FALSE,on=.(NAICS,Commodity_SCTG,BuyerID)]
    new_pc <- rbindlist(lapply(1:n_splits, sample_data, pc_data = pc, samplingforSeller = TRUE))

    pc <- rbindlist(list(pc,new_pc))

    if(new_pc[,.N]>0){
      NAICSMarkettodo[new_pc[,.N,.(NAICS,Commodity_SCTG)],status:=0,on=.(NAICS,Commodity_SCTG)]
    }

    NAICSMarkettodo[NAICSmarketnottodo,status:=0,on=.(NAICS,Commodity_SCTG)]

    if(NAICSMarkettodo[status==1,.N]>0){
      print("Making all the sellers and buyers available for sampling")
      prodcg[,availableForSample:=TRUE]
      prodcg[NAICSmarketnottodo,availableForSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
      conscg[,doSample:=TRUE]
      conscg[NAICSmarketnottodo, doSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
      new_pc <- rbindlist(lapply(1:n_splits, sample_data, pc_data = pc, samplingforSeller = TRUE))

      pc <- rbindlist(list(pc,new_pc))

      if(new_pc[,.N]==0){
        stop("No more sampling left")
      }
    }



    print(paste0("Number of new pairs: ", pc[,.N]-totalPairs))

    totalPairs <- pc[,.N]
    print(paste0("Total number of pairs: ", pc[,.N]))

    NAICSMarkettodo[,status:=1]

    rm(new_pc)
    gc()

    solution <- pc[!NAICSmarketnottodo,on=.(NAICS,Commodity_SCTG)][,.(status = SolveFlowCLP(.SD, firstrun = FALSE)),by=.(NAICS,Commodity_SCTG),.SDcols = c("SellerID","BuyerID","OutputCapacityTons","PurchaseAmountTons", "NAICS", "Commodity_SCTG")]


    SellersToDo <- solution[seq(1,.N,by=3),rbindlist(status),.(NAICS,Commodity_SCTG)]
    BuyersToDo <- solution[seq(2,.N,by=3),rbindlist(status),.(NAICS,Commodity_SCTG)]
    solution <- solution[seq(3,.N,by=3)]

    NAICSmarketnottodo <- rbindlist(list(NAICSmarketnottodo,solution[status==0,.(NAICS,Commodity_SCTG)]))

    SellersToDo <- SellersToDo[!NAICSmarketnottodo,on=.(NAICS,Commodity_SCTG)]
    BuyersToDo <- BuyersToDo[!NAICSmarketnottodo,on=.(NAICS,Commodity_SCTG)]

    prodratio <- pc[,.(dfTons=sum(PurchaseAmountTons)),.(SellerID,OutputCapacityTons)][,.(SellerID, Ratio=dfTons/OutputCapacityTons)]

    # Update the ratioweights
    prodcg[prodratio,ratioweights:=1/i.Ratio,on="SellerID"]
    prodcg[!prodratio,ratioweights:=100,on="SellerID"]

    # prodcg[NAICSmarketnottodo,availableForSample:=FALSE,on=.(NAICS,Commodity_SCTG)]
    # conscg[NAICSmarketnottodo,doSample:=FALSE, on=.(NAICS,Commodity_SCTG)]
    conscg[doSample==TRUE,samplesize:=(samplesize+20)]
    print(conscg[,.N,.(NAICS,Commodity_SCTG,samplesize)])

    print(isInfeasible <- solution[,any(status>0)])
    print(buyeriter <- buyeriter+1)
    print(paste0("Total number of buyer supplier pairs: ", pc[,.N]))
  }

  lapply(lp_problems, delProbCLP)
  rm(lp_problems)
  gc()
  # Start sampling to match modal target share
  prodcg[,availableForSample:=TRUE]
  conscg[,doSample:=TRUE]

  pc_proportion <- zonemodeavailability[pc[,.(Production_zone,Consumption_zone,Commodity_SCTG,BuyerID,PurchaseAmountTons,ODSegment)],on=.(Production_zone,Consumption_zone,Commodity_SCTG)]

  modenames <- intersect(c("Truck","Rail","Water","Air","Pipeline"),colnames(pc_proportion))



  pc_proportion <- melt.data.table(pc_proportion[,lapply(.SD,function(x) sum(x*PurchaseAmountTons)/sum(PurchaseAmountTons)),.SDcols=modenames,by=.(ODSegment,Commodity_SCTG)],id.vars = c("ODSegment","Commodity_SCTG"), variable.name = "Mode.Domestic", variable.factor = FALSE, value.name = "Proportion")

  print(pc_proportion)

  # Make a copy to compare improvements from further iterations
  pc_proportion_prior <- copy(pc_proportion)

  # Make a copy to track changes to the target share
  modal_targets_orig <- copy(modal_targets)

  # Only sample pairs from those that have observed share less than target share
  modal_targets[pc_proportion,Target:=ifelse(Target<Proportion,1e-10,Target),on=.(ODSegment,Commodity_SCTG,Mode.Domestic)]

  # Assign modal weights based on the cost between zones
  modal_weights <- longcskims_sctg[,.(weights=1/min(cost)),.(Production_zone,Consumption_zone,Mode.Domestic,ODSegment,Commodity_SCTG)]

  # Weight the modal weights by targets
  modal_weights <- merge(modal_weights,modal_targets[Target>1e-10],by=c("Mode.Domestic","ODSegment","Commodity_SCTG"),allow.cartesian = TRUE)

  modal_weights <- modal_weights[,.(modalweights=sum(weights*Target)),.(Production_zone,Consumption_zone,ODSegment,Commodity_SCTG)]


  resampleModal <- TRUE
  sampleModes <- 1L
  prior_diff <- 1
  limitBuyersResampling <- 10L

  resampleModal <- modal_targets[,any(Target>1e-10)]

  print("4. Sampling to match modal share targets")
  while(resampleModal & (prior_diff >= 1e-3) & sampleModes < (limitBuyersResampling+1L)){
    new_pc <- rbindlist(lapply(1:n_splits,sample_data,fractionOfSupplierperBuyer=1.0,samplingforSeller=resampleModal,pc_data=pc))

    pc <- rbindlist(list(pc,new_pc))
    rm(new_pc)
    gc()

    pc_proportion <- zonemodeavailability[pc[,.(Production_zone,Consumption_zone,Commodity_SCTG,BuyerID,PurchaseAmountTons,ODSegment)],on=.(Production_zone,Consumption_zone,Commodity_SCTG)]

    modenames <- intersect(c("Truck","Rail","Water","Air","Pipeline"),colnames(pc_proportion))



    pc_proportion <- melt.data.table(pc_proportion[,lapply(.SD,function(x) sum(x*PurchaseAmountTons)/sum(PurchaseAmountTons)),.SDcols=modenames,by=.(ODSegment,Commodity_SCTG)],id.vars = c("ODSegment","Commodity_SCTG"), variable.name = "Mode.Domestic", variable.factor = FALSE, value.name = "Proportion")

    print(pc_proportion)

    print(prior_diff <- pc_proportion_prior[pc_proportion,sum(abs(Proportion-i.Proportion)),on=.(ODSegment,Commodity_SCTG,Mode.Domestic)])
    # print(pc_proportion)
    pc_proportion_prior <- copy(pc_proportion)

    modal_targets <- pc_proportion[modal_targets_orig,.(Target=ifelse(Target<Proportion,1e-10,Target),ODSegment,Commodity_SCTG,Mode.Domestic),on=.(ODSegment,Commodity_SCTG,Mode.Domestic)]#[modal_targets,.(Target=ifelse(Target>1e-10,Target+i.Target,1e-10),ODSegment,Commodity_SCTG,Mode.Domestic),on=.(ODSegment,Commodity_SCTG,Mode.Domestic)]

    resampleModal <- modal_targets[,any(Target>1e-10)]

    modal_weights <- longcskims_sctg[,.(weights=1/min(cost)),.(Production_zone,Consumption_zone,Mode.Domestic,ODSegment,Commodity_SCTG)]

    modal_weights <- merge(modal_weights,modal_targets[Target>1e-10],by=c("Mode.Domestic","ODSegment","Commodity_SCTG"),allow.cartesian = TRUE)

    modal_weights <- modal_weights[,.(modalweights=sum(weights*Target)),.(Production_zone,Consumption_zone,ODSegment,Commodity_SCTG)]
    print(modal_targets)

    print(sampleModes <- sampleModes + 1L)

    print(paste0("Number of Combinations after ", sampleModes-1, " iteration: ",pc[,.N]))

  }
  pc[,c("Distance_Bin","samplesize"):=NULL]
  gc()
  print(paste(Sys.time(), "Organizing data for distribution channel model"))

  #Distribution size model

  #buyer/seller attributes

  # Distribution Channel (Calibration) Model
  if(model$scenvars$distchannelCalibration){
    pc <- pc_sim_distchannel(pc = pc, distchannel_food = distchan_food, distchannel_mfg = distchan_mfg, calibration = TRUE)
  } else {
    pc <- pc_sim_distchannel(pc = pc, distchannel_food = distchan_food, distchannel_mfg = distchan_mfg)
  }


  ### -- Heither, 10-15-2015: Enforce Distribution channel logical consistency -- ###

  pc[distchannel==1 & Production_zone %in% c(179:180) & !Consumption_zone %in% c(179:180), distchannel := 3]				### -- Hawaii origin - non-Hawaii destination

  pc[distchannel==1 & !Production_zone %in% c(179:180) & Consumption_zone %in% c(179:180), distchannel := 3]				### -- non-Hawaii origin - Hawaii destination

  pc[distchannel==1 & Production_zone<=273 & Consumption_zone>273 & !Consumption_zone %in% c(310,399), distchannel := 3]	### -- U.S. origin - foreign destination (except Canada/Mexico)

  pc[distchannel==1 & Production_zone>273 & !Production_zone %in% c(310,399) & Consumption_zone<=273, distchannel := 3]		### -- foreign origin (except Canada/Mexico) - U.S. destination


  # Create set of shipment size options and expand the pc table by that. The optimal shipment
  # size will be selected in mode choice -- minimun logistics cost for the combination of shipment
  # size and path.
  ##########################
  ## This part of code is memory intensive
  ###########################

  # pc[, k:= 1 ]
  # pc <- merge(pc, ShipSize, by = "k", allow.cartesian = TRUE)
  #
  # # Clean up fields -- remove temps fields from distrubtion channel model
  # pc[, k:= NULL]

  #Calculate Costs and Times for Each Mode-Path Alternative

  #Each shipper-receiver pair selects one transport & logistics path for its shipping needs based on annual transport & logistics costs

  ## ---------------------------------------------------------------

  ## Heither, revised 07-22-2015

  print(paste(Sys.time(), "Applying the mode-path choice model"))

  setkey(pc, Production_zone, Consumption_zone)

  # pc[, lssbd := ifelse(Seller.Size > 5 & Buyer.Size < 3 & Distance > 300, 1, 0)]
  # faster computation of above code.
  pc[Seller.Size>5 & Buyer.Size < 3 & Distance > 300, lssbd:= 1]
  pc[!(Seller.Size>5 & Buyer.Size < 3 & Distance > 300),lssbd:=0]

  #Add "MinGmnql","MinPath","Attribute2_ShipTime" to pc using the minLogisticsCost function

  ##Rprof("profile1.out")


  size_per_row <- 12010 #bytes
  npct <- as.numeric(pc[,.N])
  nShipSizet <- as.numeric(ShipSize[Commodity_SCTG %in% pc[,unique(Commodity_SCTG)],.N])
  n_splits <- ceiling(npct*nShipSizet*size_per_row/(ram_to_use/cores_used))
  suppressWarnings(pc[,':='(n_split=1:n_splits,k=1)])


  findMinLogisticsCost <- function(split_number,modeChoiceConstants=NULL){
    pc_split <- pc[n_split==split_number]
    pc_split <- merge(pc_split,ShipSize,by=c("Commodity_SCTG","k"),allow.cartesian=TRUE,all.x=TRUE)
    pc_split[,k:=NULL]
    setkey(pc_split, Production_zone, Consumption_zone)
    # print(paste0(object.size(pc_split)/(1024**3)," Gb"))
    if(!is.null(modeChoiceConstants)){
      df_fin <- minLogisticsCost(pc_split,0, naics, modeChoiceConstants = modeChoiceConstants, recycle_check_file_path)
    } else {
      df_fin <- minLogisticsCost(pc_split,0, naics, modeChoiceConstants=NULL, recycle_check_file_path)
    }
    setnames(df_fin,c("time","path","minc"),c("Attribute2_ShipTime","MinPath","MinGmnql"))
    # setkey(df_fin,SellerID,BuyerID,NAICS,Commodity_SCTG, weight)
    # setkey(pc_split,SellerID,BuyerID,NAICS,Commodity_SCTG, weight)
    commonNames <- intersect(colnames(pc_split),colnames(df_fin))
    return(pc_split[df_fin,on=commonNames])
  }

  pc[FAFZONE.buyer==FAFZONE.supplier,ODSegment:="I"]
  pc[FAFZONE.buyer!=FAFZONE.supplier,ODSegment:="X"]
  iter <- 1
  if(model$scenvars$modechoiceCalibration){
    c_path_mode <- mode_description[,.(path=ModeNumber,Mode.Domestic=Mode)]
    c_path_mode[Mode.Domestic=="Multiple",Mode.Domestic:="Rail"]
    c_path_mode[,ShipmentType:=ifelse(path %in% c(3,13,31,46,55:57),"D","ID")]
    pipeline_mode <- c_path_mode[c_path_mode[,.I[Mode.Domestic=="Pipeline"]]][,ShipmentType:="ID"]
    c_path_mode <- rbind(c_path_mode,pipeline_mode)
    # faf_modeshare[Mode.Domestic=="Multiple",Mode.Domestic:="Rail"]
    # faf_modeshare <- faf_modeshare[Mode.Domestic!="Other"]
  }
  if(model$scenvars$modechoiceCalibration){
    modeChoiceConstants <- data.table(expand.grid.df(data.frame(Commodity_SCTG=pc[,unique(Commodity_SCTG)]),data.frame(ODSegment=c("I","X")), c_path_mode))
    modeChoiceConstants[,Constant:=0]
    # modeChoiceConstants[,ShipmentType:=ifelse(path %in% c(3,13,31,46,55:57),"D","ID")]
    modeChoiceConstants[,N:=.N,by=.(ODSegment,Mode.Domestic,ShipmentType)]
    Targets <- pc_domestic_targets[Commodity_SCTG %in% unique(pc$Commodity_SCTG)]
    iter <- 4
  }

  for(iters in 1:iter){
    if(model$scenvars$modechoiceCalibration|exists("modeChoiceConstants")){
      df_min <- rbindlist(lapply(1:n_splits,findMinLogisticsCost,modeChoiceConstants=modeChoiceConstants))
    } else {
      df_min <- rbindlist(lapply(1:n_splits,findMinLogisticsCost))
    }
    if(model$scenvars$modechoiceCalibration){
      df_share <- df_min[!(Production_zone > 273 | Consumption_zone > 273), .(dfTons = sum(PurchaseAmountTons)),by = .(Commodity_SCTG, ODSegment,path=MinPath,ShipmentType=(distchannel==1 |(Production_zone<151 & Consumption_zone<151)))]
      df_share[,ShipmentType:=ifelse(ShipmentType,"D","ID")]
      df_share[is.na(dfTons),dfTons:=0]
      df_share[c_path_mode, c("Mode.Domestic"):=.(i.Mode.Domestic), on = c("path","ShipmentType")]
      shipmenttypeshare <- df_share[,.(dfTons=sum(dfTons)),.(Commodity_SCTG,ODSegment,ShipmentType)]
      shipmenttypeshare[,ShipmentShare:=prop.table(dfTons),.(Commodity_SCTG,ODSegment)]
      df_share <- df_share[,.(dfTons=sum(dfTons)), by= .(Commodity_SCTG,ODSegment,ShipmentType,Mode.Domestic)]
      df_share[,ModTons:=sum(dfTons,na.rm=TRUE), by= .(Commodity_SCTG,ODSegment,Mode.Domestic)]
      df_share[shipmenttypeshare,ShipmentShare:=i.ShipmentShare,on=.(Commodity_SCTG,ODSegment,ShipmentType)]
      df_share <- merge(df_share,Targets,by=c("Commodity_SCTG","ODSegment","Mode.Domestic"), all=TRUE)
      df_share[is.na(ShipmentType),ShipmentType:="ID"]
      df_share[shipmenttypeshare,ShipmentShare:=i.ShipmentShare,on=.(Commodity_SCTG,ODSegment,ShipmentType)]

      df_share[, c("Mod","ModTotal"):= .(dfTons/sum(dfTons,na.rm=TRUE),ModTons/sum(dfTons,na.rm=TRUE)),by=.(Commodity_SCTG,ODSegment)]
      # df_share[, Mod:= dfTons/sum(dfTons,na.rm=TRUE),by=.(Commodity_SCTG,ODSegment)]
      df_share[is.na(Mod) | Mod <= 1E-5, Mod := 1E-5]
      df_share[is.na(ModTotal) | ModTotal <= 1E-5, ModTotal := 1E-5]
      df_share[is.na(Target) | Target <= 1E-5, Target := 1E-5]
      df_share[Mode.Domestic %in% c("Truck","Rail","Pipeline"),TargetMultiply:=TRUE]
      df_share[!Mode.Domestic %in% c("Truck","Rail","Pipeline"),TargetMultiply:=FALSE]
      df_share[,TargetTotal:=Target]
      df_share[,ReduceShare:=Target*(!all(TargetMultiply)),.(Commodity_SCTG,ODSegment,Mode.Domestic)]
      df_share[,ShipmentShare:=ShipmentShare-sum(ReduceShare),.(Commodity_SCTG,ODSegment,ShipmentType)]
      df_share[TargetMultiply==TRUE,Target:=ShipmentShare*prop.table(TargetTotal),.(Commodity_SCTG,ODSegment,ShipmentType)]
      df_share[,c("Mod","Target"):= .(prop.table(Mod),prop.table(Target)),.(Commodity_SCTG,ODSegment,ShipmentType)]
      df_share[, Adj := 0]
      df_share[, Adj1 := log(Target/Mod)]
      df_share[, Adj2 := abs(log(TargetTotal/ModTotal))]
      df_share[, Adj := ifelse(Adj2 < 1E-2, Adj1 * Adj2, Adj1)]
      # print(df_share[,.(Commodity_SCTG,ODSegment,Mode.Domestic,ShipmentType,Mod,Target,ModTotal,TargetTotal,Adj1,Adj2, Adj)])
      # print(df_share[,.(Commodity_SCTG,ODSegment,Mode.Domestic,ShipmentType,Mod,Target,ModTotal,TargetTotal,Adj1,Adj2, Adj)])
      # save(df_min,df_share,modeChoiceConstants, file=file.path(model$outputdir,paste0(naics,"_g",g,"_iter",iters,".RData")))
      # df_share[modeChoiceConstants[,.N,by=.(Commodity_SCTG,ODSegment,Mode.Domestic)],Adj:=Adj/i.N]
      modeChoiceConstants[df_share,Constant:=Constant+(i.Adj/N), on = c("Commodity_SCTG","ODSegment","ShipmentType","Mode.Domestic")]
    }
  }
  pc <- df_min
  gc()
  if(model$scenvars$modechoiceCalibration){
    saveRDS(modeChoiceConstants[,':='(NAICS=naics,Group=g)], file=file.path(model$inputdir, paste0(naics,"_g",g,"_","modechoiceconstants.rds")))
  }
  pc[,c("n_split"):=NULL]
  ## ---------------------------------------------------------------

  ## Heither, revised 02-05-2016: revised so correct modepath is reported

  # df_fin <- minLogisticsCost(pc,0, naics, recycle_check_file_path)

  # setnames(df_fin,c("time","path","minc"),c("Attribute2_ShipTime","MinPath","MinGmnql"))
  #
  # setkey(df_fin,SellerID,BuyerID,NAICS,Commodity_SCTG, weight)
  #
  # setkey(pc,SellerID,BuyerID,NAICS,Commodity_SCTG, weight)
  #
  # pc <- pc[df_fin]

  setkey(pc,Production_zone,Consumption_zone)	### return to original sort order



  ## ~~~~~~~~~~~~~~~~~~~



  ##Rprof(NULL)

  ##profsum <- summaryRprof("profile1.out")

  pc[, Attribute1_UnitCost := MinGmnql / PurchaseAmountTons]

  pc[, Attribute2_ShipTime := Attribute2_ShipTime / 24] #Convert from hours to days



  ## ---------------------------------------------------------------



  #Prepare and write the cost file

  print(paste(Sys.time(), "Preparing and writing the costs file for",naics))



  #save the full table to an rdata file for use after the PMG game

  save(pc, file = file.path(model$outputdir,paste0(naics, "_g", g, ".Rdata")))



  # Clean up unneeded variables

  pc[, c("Production_zone", "Consumption_zone", "Commodity_SCTG", "NAICS","Seller.NAICS", "Seller.Size", "Buyer.NAICS",

         "Buyer.SCTG","Buyer.Size","ConVal","PurchaseAmountTons", "OutputCapacityTons",

         "weight", "Distance", "Ship_size", "distchannel",

         "lssbd", "MinGmnql", "MinPath","FAFZONE.supplier","FAFZONE.buyer","Distance_Bin","ODSegment") := NULL]



  #For possible buyer and supplier pairs, build a file

  #BuyerID

  #SellerID

  #Attribute1_UnitCost

  #Attribute2_ShipTime

  #Attribute3_K

  #Attribute4_L

  #Attribute5_M

  #Tag1_SingleSource

  #Tag2_PortOfOrigin

  #Tag3_PortOfEntry



  #pc[, Tag1_SingleSource := SellerID]

  #pc[, c("Attribute3_K", "Attribute4_L", "Attribute5_M") := 0]

  #pc[, c("Tag2_PortOfOrigin", "Tag3_PortOfEntry") := ""]



  #write out the costs file for this group
  # print(paste0("Number of rows less than ",model$scenvars$combinationthreshold,": ",pc[,.N]<model$scenvars$combinationthreshold))

  fwrite(pc, file = file.path(model$outputdir,paste0(naics, "_g", g, ".costs.csv")))



  ## Heither, revised 10-05-2015: File Cleanup if Outputs folder being re-used from previous run

  ## -- Delete NAICS_gX.out file if exists from prior run (existence will prevent PMG from running)

  if(file.exists(file.path(outputdir,paste0(naics,"_g", g, ".out.csv")))){

		file.remove(file.path(outputdir,paste0(naics,"_g", g, ".out.csv")))

  }

  ## -- Delete NAICS_gX.txt file if exists from prior run as well

  if(file.exists(file.path(outputdir,paste0(naics,"_g", g, ".txt")))){

		file.remove(file.path(outputdir,paste0(naics,"_g", g, ".txt")))

  }





  rm(pc,prodcg,conscg)

  gc()



  print(paste(Sys.time(), "Complete for", naics, "group", g, "in", (proc.time() - starttime)[3]))

}



#save current workspace for use by seperate R script processes for running NAICS groups

save.image(file = file.path(model$outputdir,"PMG_Workspace.Rdata"))



pmg <- progressEnd(pmg)
