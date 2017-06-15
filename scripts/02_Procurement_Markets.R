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

ShipSize <- data.table(Ship_size = 1:4, weight = c(1000L, 10000L, 30000L, 150000L), k = 1)

 

## ---------------------------------------------------------------

## Heither, revised 09-25-2015: CMAP pre-processing procedures create time & cost fields for all 54 mode paths for

##                              ALL Domestic and Foreign combinations.  RSG shortcut code is now turned off.



#### -- OBSOLETE: CMAP only skims, time and cost fields for 54 paths

setnames(skims, c("Origin", "Destination"), c("Production_zone", "Consumption_zone"))

#### -- setkey(skims, Production_zone, Consumption_zone)

if(!identical(nModes <- sum(grepl("time",colnames(skims))),nCost <- sum(grepl("cost",colnames(skims))))) stop(paste0("Information of time and cost available on ", min(nCost,nModes),"/",max(nCost,nModes)))

cskims <- skims[,c("Production_zone", "Consumption_zone",paste0("time", 1:nModes), paste0("cost", 1:nModes)),with = FALSE]


setkey(cskims, Production_zone, Consumption_zone)

rm(skims)

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



## ---------------------------------------------------------------

## Heither, revised 07-24-2015: added model$scenvars variables, corrected B3 portion of function to include j, corrected

##                              final line of equation to implement CS code

#Define the logistics cost function used in the mode path model

calcLogisticsCost <- function(dfspi,s,path){
  
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

minLogisticsCostSctgPaths <- function(dfsp,iSCTG,paths, naics, recycle_check_file_path){
  

  callIdentifier <- paste0("minLogisticsCostSctgPaths(iSCTG=",iSCTG, ", paths=", paste0(collapse=", ", paths), " naics=", naics, ")")

  startTime <- Sys.time()

  # print(paste0(startTime, " Entering: ", callIdentifier, " nrow(dfsp)=", nrow(dfsp)))

    s <- sctg[iSCTG]

    dfsp <- merge(dfsp,cskims[,c("Production_zone","Consumption_zone",paste0("time",paths),paste0("cost",paths)),with=F])

    numrows <- nrow(dfsp)
    #################
    # Full approach
    ##################
    transform_merge_data <- function(path){
      dfsp_data <- cbind(melt(dfsp[,c("Production_zone","Consumption_zone","SellerID","BuyerID","NAICS","Commodity_SCTG","PurchaseAmountTons","weight","ConVal","lssbd",paste0("time",path)),with=F], measure.vars=paste0("time",path), variable.name="timepath", value.name="time"),

                    melt(dfsp[,paste0("cost",path),with=F], measure.vars=paste0("cost",path), variable.name="costpath", value.name="cost"))
      # print(paste0("Path: ",path))
      # print(paste0("Full: ", object.size(dfsp_data)/(1024**3)," Gb"))
      dfsp_data <- dfsp_data[!(is.na(time)&is.na(cost))]
      # print(paste0("Partial: ", object.size(dfsp_data)/(1024**3)," Gb"))
      suppressWarnings(dfsp_data[,path:=as.numeric(path)])
    }

    dfsp <- rbindlist(lapply(paths,transform_merge_data)) # Saves memory with a little compromise on time
    
    #################
    # Group approach
    ################
    # dfsp <- cbind(melt(dfsp[,c("Production_zone","Consumption_zone","SellerID","BuyerID","NAICS","Commodity_SCTG","PurchaseAmountTons","weight","ConVal","lssbd",paste0("time",paths)),with=F], measure.vars=paste0("time",paths), variable.name="timepath", value.name="time"),
    # 
    #               melt(dfsp[,paste0("cost",paths),with=F], measure.vars=paste0("cost",paths), variable.name="costpath", value.name="cost"))
    # suppressWarnings(dfsp[,path:=as.numeric(rep(paths,each=numrows))])
    # print(paste0("Paths: ",paste0(paths,collapse = ",")))
    # print(paste0("Full: ", object.size(dfsp)/(1024**3)," Gb"))
    # dfsp <- dfsp[!(is.na(time)&is.na(cost))]
    # print(paste0("Partial: ", object.size(dfsp)/(1024**3)," Gb"))
    

    dfsp[,minc:=calcLogisticsCost(.SD[,list(PurchaseAmountTons,weight,ConVal,lssbd,time,cost)],s,unique(path)),by=path] # Faster implementation of above code

    ## variables from model$scenvars

    CAP1Carload  <- model$scenvars$CAP1Carload    #Capacity of 1 Full Railcar

    CAP1FTL      <- model$scenvars$CAP1FTL		#Capacity of 1 Full Truckload

    CAP1Airplane <- model$scenvars$CAP1Airplane	#Capacity of 1 Airplane Cargo Hold



  dfsp[,avail:=TRUE]

  dfsp[path %in% 1:12 & weight<CAP1Carload,avail:=FALSE] #Eliminate Water and Carload from choice set if Shipment Size < 1 Rail Carload

  dfsp[path %in% c(14,19:26,31) & weight<CAP1FTL,avail:=FALSE] #Eliminate FTL and FTL-IMX combinations from choice set if Shipment Size < 1 FTL

  dfsp[path %in% c(32:38) & weight<CAP1FTL,avail:=FALSE] #Eliminate FTL Linehaul with FTL external drayage choice set if Shipment Size < 1 FTL (Heither, 11-06-2015)

  dfsp[path %in% c(15:18,27:30,39:46) & weight>CAP1FTL,avail:=FALSE] #Eliminate LTL and its permutations from choice set if Shipment Size > FTL (Heither, 11-06-2015)

  dfsp[path %in% 47:50 & weight>CAP1Airplane,avail:=FALSE] #Eliminate Air from choice set if Shipment Size > Air Cargo Capacity

  dfsp[path %in% 51:52 & weight<(0.75*CAP1FTL),avail:=FALSE] #Eliminate Container-Direct from choice set if Shipment Size < 1 40' Container

  dfsp[path %in% 53:54 & weight<CAP1FTL,avail:=FALSE] #Eliminate International Transload-Direct from choice set if Shipment Size < 1 FTL
  
  dfsp[!(iSCTG %in% c(16:19)) & (path %in% c(55:57)), avail:=FALSE]

  dfsp <- dfsp[avail==TRUE & !is.na(minc)]		## limit to only viable choices before finding minimum path

  dfsp <- dfsp[dfsp[,.I[which.min(minc)],by=list(SellerID,BuyerID)][,V1],]

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

minLogisticsCost <- function(df,runmode, naics, recycle_check_file_path){

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

		df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(3,13,31,46), naics, recycle_check_file_path)

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

		df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(1:2,4:12,14:30,32:45,47:54,55:57),naics, recycle_check_file_path)

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

		df2 <- minLogisticsCostSctgPaths(df1,iSCTG,c(1:2,4:12,14:30,32:45,55:57), naics, recycle_check_file_path)			## include inland water - 03-06-2017

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

## ---------------------------------------------------------------
######################
### Edition with 1 group per NAICS group
#####################
# create_pmg_sample_groups <- function(naics,groups,sprod){
#   # sort by sizes
# 
#   setkey(consc, Size)
# 
#   setkey(prodc, Size)
# 
#   prodconsratio <- sum(prodc$OutputCapacityTons)/sum(consc$PurchaseAmountTons)
# 
#   if(prodconsratio < 1.1){ #TODO need to move this to variables
# 
#     #reduce consumption to ratio is >=1.1
# 
#     suppressWarnings(consc[,PurchaseAmountTons:=PurchaseAmountTons/1.1*prodconsratio])
# 
#   }
# 
# 
#   #merge together to create all of the pairs of firms for this group and commodity
# 
# 
#   set.seed(151)
#   suppressWarnings(consc[,group:=1:groups])
# }

######################
### Edition with multiple groups per NAICS group
#####################


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

# pc_sim_distchannel <- function(pcFlows, distchannel_food, distchannel_mfg, c_sctg_cat, calibration = NULL){
#   # Update progress log
#   
#   ### Create variables used in the distribution channel model
#   
#   # Create employment and industry dummy variables
#   pcFlows[, c("emple49", "emp50t199", "empge200", "mfgind", "trwind", "whind") := 0L]
#   pcFlows[Buyer.Size <= 49, emple49 := 1]
#   pcFlows[Buyer.Size >= 50 & Buyer.Size <= 199, emp50t199 := 1]
#   pcFlows[Buyer.Size >= 200, empge200 := 1]
#   
#   pcFlows[,Seller.NAICS2:=substr(Seller.NAICS,1,2)]
#   pcFlows[Seller.NAICS2 %in% 31:33, mfgind := 1]
#   pcFlows[Seller.NAICS2 %in% 48:49, trwind := 1]
#   pcFlows[Seller.NAICS2 %in% c(42, 48, 49), whind := 1]
#   
#   pcFlows[,Buyer.NAICS2:=substr(Buyer.NAICS,1,2)]
#   pcFlows[Buyer.NAICS2 %in% 31:33, mfgind := 1]
#   pcFlows[Buyer.NAICS2 %in% 48:49, trwind := 1]
#   pcFlows[Buyer.NAICS2 %in% c(42, 48, 49), whind := 1]
#   
#   
#   # Add the FAME SCTG category for comparison with calibration targets
#   pcFlows[, CATEGORY := famesctg[Commodity_SCTG]]
#   setkey(pcFlows, Production_zone, Consumption_zone)
#   
#   # Add zone to zone distances
#   pcFlows <- merge(pcFlows, mesozone_gcd, c("Production_zone", "Consumption_zone")) # append distances
#   setnames(pcFlows, "GCD", "Distance")
#   
#   # Update progress log
#   # Missing for now
#   
#   
#   print(paste(Sys.time(), "Applying distribution channel model"))
#   
#   #Apply choice model of distribution channel and iteratively adjust the ascs
#   
#   #The model estimated for mfg products was applied to all other SCTG commodities
#   
#   inNumber <- nrow(pc[Commodity_SCTG %in% c(1:9)])
#   
#   if (inNumber > 0) {
#     
#     # Sort on vars so simulated choice is ordered correctly
#     model_vars_food <- c("CATEGORY", distchannel_food[TYPE == "Variable", unique(VAR)])
#     model_ascs_food <- distchannel_food[TYPE == "Constant", unique(VAR)]
#     setkeyv(pcFlows, model_vars_food) #sorted on vars, calibration coefficients, so simulated choice is ordered correctly
#     
#     pcFlows_food <- pcFlows[Commodity_SCTG %in% c(1:9),model_vars_food,with=FALSE]
#     pcFlows_food_weight <- pcFlows[SCTG %in% 1:9,Tons]
#     
#     df <- pcFlows_food[, list(Start = min(.I), Fin = max(.I)), by = model_vars_food, with=FALSE] #unique combinations of model coefficients
#     
#     df[, (model_ascs_food) := 1] #add 1s for constants to each group in df
#     
#     print(paste(Sys.time(), nrow(df), "unique combinations"))
#     
#     if(!is.null(calibration)){
#       
#       pcFlows[SCTG %in% c(1:9), DistChannel := predict_logit(df, distchannel_food, cal = distchan_cal, calcats = distchan_calcats, weight = pcFlows_food_weight, iter=4, path = file.path(model$inputdir,"model_distchannel_food_cal.csv"))]
#       
#     } else {
#       
#       pcFlows[SCTG %in% 1:9, DistChannel := predict_logit(df, distchannel_food,distchan_cal,distchan_calcats,iter=4)]  
#       
#     }
#     # 
#     # pc[Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchan_food, distchan_cal, distchan_calcats, 4)]
#     
#   }
#   
#   # Update progress log
#   
#   
#   print(paste(Sys.time(), "Finished ", inNumber, " for Commodity_SCTG %in% c(1:9)"))
#   
#   ### Apply choice model of distribution channel for other industries
#   
#   # The model estimated for mfg products is applied to all other SCTG commoditie
#   
#   outNumber <- nrow(pcFlows[!Commodity_SCTG %in% c(1:9)])
#   
#   if (outNumber > 0) {
#     
#     # Sort on vars so simulated choice is ordered correctly
#     model_vars_mfg <- c("CATEGORY", distchannel_mfg[TYPE == "Variable", unique(VAR)])
#     model_ascs_mfg <- distchannel_mfg[TYPE == "Constant", unique(VAR)]
#     
#     setkeyv(pc, model_vars_mfg) #sorted on vars so simulated choice is ordered correctly
#     
#     pcFlows_mfg <- pcFlows[!Commodity_SCTG %in% c(1:9),model_vars_mfg,with=FALSE]
#     pcFlows_mfg_weight <- pcFlows[!Commodity_SCTG %in% c(1:9),Tons]
#     
#     df <- pcFlows_mfg[, list(Start = min(.I), Fin = max(.I)), by = model_vars_mfg] #unique combinations of model coefficients
#     
#     ####do this in the function (seems unecessary here)?
#     
#     df[, (model_ascs_mfg) := 1] #add 1s for constants to each group in df
#     
#     print(paste(Sys.time(), nrow(df), "unique combinations"))
#     
#     # Simulate choice -- with calibration if calibration targets provided
#     if(!is.null(calibration)){
#       
#       pcFlows[!SCTG %in% c(1:9), DistChannel := predict_logit(df, distchannel_mfg, cal = distchan_cal, calcats = distchan_calcats, weight = pcFlows_mfg_weight, iter=4)]
#       
#     } else {
#       
#       pcFlows[!SCTG %in% 1:9, DistChannel := predict_logit(df, distchannel_mfg,distchan_cal,distchan_calcats,iter=4)]
#       
#     }
#     
#     # pc[!Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchan_mfg, distchan_cal, distchan_calcats, 4)]
#     
#   }
#   
#   rm(df)
#   print(paste(Sys.time(), "Finished ", outNumber, " for !Commodity_SCTG %in% c(1:9)"))
#   
#   
#   # Update progress log
#   
#   return(pcFlows)
#   
#   
# }




predict_logit <- function(df,mod,cal=NULL,calcats=NULL,weight=NULL,iter=1,path=NULL){

  #prepare the data items used in the model application and calibration
  alts <- max(mod$CHID)
  ut<-diag(alts)
  ut[upper.tri(ut)] <- 1
  
  if(is.numeric(df$CATEGORY)) df[,CATEGORY:=paste0("x",CATEGORY)] #numeric causes problems with column names

  cats <- unique(df$CATEGORY)
  mod<-data.table(expand.grid.df(mod,data.frame(CATEGORY=cats)))
  
  if(is.numeric(cal$CATEGORY)) cal[,CATEGORY:=paste0("x",CATEGORY)]
  
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
      

      if(length(unique(calcats$CHOICE))<length(unique(calcats$CHID))) {#the sim choices need to be aggregated to the calibration data
        sim <- cbind(calcats,sim)
        sim <- melt(sim,id.vars=c("CHOICE","CHID"),variable.name="CATEGORY")
        sim <- sim[,list(MODEL=sum(value)),by=list(CHOICE,CATEGORY)]
        sim <- merge(sim,cal,c("CATEGORY","CHOICE"))
        sim[MODEL==0,MODEL:=0.001] # Stop dividing it with 0
        sim[,ascadj:=log(TARGET/MODEL)]
        adj <- merge(sim,calcats,"CHOICE",allow.cartesian=TRUE)[,list(CATEGORY,CHID,ascadj)]
      }

      if(length(unique(calcats$CHOICE))>length(unique(calcats$CHID))) {#the calibratin data need to be aggregated to the sim choices
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
    if(iter>1) modcoeffs[[length(modecoeffs)+1]] <- mod
  }
  if(iter > 1) fwrite(modcoeffs[[length(modecoeffs)]], file=path)
  return(simchoice)

}


create_pmg_inputs <- function(naics,g,sprod, recycle_check_file_path){


  starttime <- proc.time()



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
    # fwrite(prodc[group==g,list(OutputCommodity,SellerID,FAFZONE,Zone,NAICS,Size,OutputCapacityTons,NonTransportUnitCost)],
    #
    #           file = file.path(model$outputdir,paste0(naics, "_g", g, ".sell.csv")))
    #
    # prodcg <- prodc[group==g,list(OutputCommodity,NAICS,Commodity_SCTG,SellerID,Size,FAFZONE,Zone,OutputCapacityTons)]

    # consamount <- sum(consc[group==g]$PurchaseAmountTons)/sum(consc$PurchaseAmountTons)

    # prodc[,OutputCapacityTonsG:= OutputCapacityTons * consamount]

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
  
  ##############################
  # Full apprach
  ##############################
  # if(sprod==1){
  #   fwrite(prodc[,list(OutputCommodity,NAICS,Commodity_SCTG,SellerID,Size,FAFZONE,Zone,OutputCapacityTons,NonTransportUnitCost)],
  #          file = file.path(model$outputdir,paste0(naics, "_g", g, ".sell.csv")))
  #   prodcg <- prodc[,list(OutputCommodity,NAICS,Commodity_SCTG,SellerID,Size,FAFZONE,Zone,OutputCapacityTons)]
  # } else {
  #   #reduce capacity based on demand in this group
  #   consamount <- sum(conscg$PurchaseAmountTons)/sum(consc$PurchaseAmountTons)
  #   prodc[,OutputCapacityTonsG:= OutputCapacityTons * consamount]
  #   fwrite(prodc[,list(OutputCommodity,SellerID,FAFZONE,Zone,NAICS,Size,OutputCapacityTons=OutputCapacityTonsG,NonTransportUnitCost)],
  #          file = file.path(model$outputdir,paste0(naics, "_g", g, ".sell.csv")))
  #   prodcg <- prodc[,list(OutputCommodity,NAICS,Commodity_SCTG,SellerID,Size,FAFZONE,Zone,OutputCapacityTons=OutputCapacityTonsG)]
  # }

  print(paste(Sys.time(), "Finished writing sell file for ",naics,"group",g))



  print(paste(Sys.time(), "Applying distribution, shipment, and mode-path models to",naics,"group",g))

  # Rename ready to merge

  setnames(conscg, c("InputCommodity", "NAICS", "Zone","Size"), c("NAICS", "Buyer.NAICS", "Consumption_zone","Buyer.Size"))

  setnames(prodcg, c("OutputCommodity", "NAICS", "Zone","Size"),c("NAICS", "Seller.NAICS", "Production_zone","Seller.Size"))

  # Aditya: This is where  I believe that the buyer supplier pairs should be generated
  distance_bins <- model$scenvars$distance_bins
  nSupplierPerBuyer <- model$scenvars$nSupplierPerBuyer
  distBased <- model$scenvars$distBased

  FAF_distance <- fread(file.path(model$basedir,"scenarios","base","inputs","FAF_distance.csv"),key = c("oFAFZONE","dFAFZONE"))
  FAF_distance[,Distance_Bin:= findInterval(distance,distance_bins)]
  if(model$scenvars$distBased){
    supplier_selection_distribution <- fread(file.path(model$basedir,"scenarios","base","inputs","DistanceDistribution.csv"),key=c("SCTG","DistanceGroup"))
  } else {
    supplier_selection_distribution <- fread(file.path(model$basedir,"scenarios","base","inputs","TonnageDistribution.csv"),key=c("SCTG","DistanceGroup"))
  }



  # merge together to create all of the pairs of firms for this group and commodity

  set.seed(151)
  cthresh <- model$scenvars$combinationthreshold
  nSupplierPerBuyer <- model$scenvars$nSuppliersPerBuyer
  cores_used <- model$scenvars$maxcostrscripts
  pc <- data.table()
  n_splits <- 1L
  nconst <- as.numeric(conscg[,.N])
  nprodt <- as.numeric(prodcg[,.N])
  size_per_row <- 104
  ram_to_use <- 80 #Gb
  n_splits <- ceiling(nconst*nprodt*size_per_row/((ram_to_use*(1024**3))/(cores_used+2)))
  # while (nconst * prodcg[,.N] > cthresh) {
  #   n_splits <- n_splits + 1L
  #   nconst <- as.numeric(ceiling(conscg[,.N] / n_splits))
  # }
  suppressWarnings(conscg[,':=' (n_split=1:n_splits,doSample=TRUE)])
  prodcg[,availableForSample:=TRUE]

  sample_data <- function(split_number,fractionOfSupplierperBuyer=1.0,samplingforSeller=FALSE){
    pc_split <- merge(prodcg[availableForSample==TRUE][,availableForSample:=NULL],conscg[n_split==split_number&doSample==TRUE][,c("n_split","doSample"):=NULL],by = c("NAICS","Commodity_SCTG"), allow.cartesian = TRUE,suffixes = c(".supplier",".buyer"))
    pc_split <- pc_split[Production_zone<=273 | Consumption_zone<=273]	### -- Heither, 10-14-2015: potential foreign flows must have one end in U.S. so drop foreign-foreign
    pc_split[,Distance_Bin:=FAF_distance[.(FAFZONE.supplier,FAFZONE.buyer),Distance_Bin]]
    pc_split[,Proportion:=supplier_selection_distribution[.(Commodity_SCTG,Distance_Bin),Proportion]]
    pc_split[is.na(Proportion),Proportion:=0.0]
    pc_split[,Proportion:=Proportion*(OutputCapacityTons/sum(OutputCapacityTons)),by=.(BuyerID,Commodity_SCTG,Distance_Bin)]
    min_proportion <- pc_split[Proportion>0,min(Proportion)]/1000
    pc_split[,Proportion:=Proportion+min_proportion]
    resample <- TRUE # Check to see all the sellers can meet the buyers demand.
    sampleIter <- 1
    while(resample&sampleIter<6) {
      sampled_pairs <- pc_split[, .(SellerID = sample(SellerID, min(.N, ceiling(nSupplierPerBuyer*fractionOfSupplierperBuyer)), replace = FALSE, prob = Proportion)), by = .(BuyerID)]
      setkey(sampled_pairs, BuyerID, SellerID)
      resample <- pc_split[sampled_pairs,on=c("BuyerID","SellerID")][,sum(OutputCapacityTons)<unique(PurchaseAmountTons),by=.(BuyerID)][,sum(V1)] > ceiling(0.01*length(pc_split[,unique(BuyerID)])) # Make sure that the buyers demand could be fulfilled.
      resample  <- resample&!samplingforSeller
      # pc_split_partial <- pc_split[sampled_pairs,on=c("BuyerID","SellerID")]
      # resample <- pc_split_partial[,sum(OutputCapacityTons)/.N,by=SellerID][,sum(V1)]<pc_split_partial[,sum(PurchaseAmountTons)/.N,by=BuyerID][,sum(V1)]
      sampleIter <- sampleIter+1
    }
    print(paste0("Number of sampling iterations for Sellers: ",sampleIter-1))
    # print(sprintf("Split Number: %d/%d",split_number,n_splits))
    # print(sprintf("Number of pc_split rows: %d", pc_split[,.N]))
    # print(paste0(object.size(pc_split)/(1024**3)," Gb"))
    # print(paste0(object.size(pc_split[sampled_pairs,on=c("BuyerID","SellerID")])/(1024**3)," Gb"))
    return(pc_split[sampled_pairs,on=c("BuyerID","SellerID")][,Proportion:=NULL])
  }
  pc <- rbindlist(lapply(1:n_splits,sample_data))
  resampleBuyers <- TRUE # Check to make sure that all buyers can meet the sellers Capacity.
  sampleBuyersIter <- 1L
  sellersMaxedOut <- pc[,sum(PurchaseAmountTons)>unique(OutputCapacityTons),by=.(SellerID)][V1==TRUE,SellerID]
  buyersOfSellers <- pc[SellerID %in% sellersMaxedOut,unique(BuyerID)]
  resampleBuyers <- (length(sellersMaxedOut)>0)
  prodcg[SellerID %in% sellersMaxedOut,availableForSample:=FALSE]
  conscg[!(BuyerID %in% buyersOfSellers), doSample:=FALSE]
  sellDifference <- pc[SellerID%in%sellersMaxedOut,sum(PurchaseAmountTons)-unique(OutputCapacityTons),by=SellerID][,sum(V1)]
  # buyDifference <- pc[BuyerID%in%buyersOfSellers,sum(OutputCapacityTons)-unique(PurchaseAmountTons),by=BuyerID][,sum(V1)]
  print(paste0("Original Number of Combinations: ", pc[,.N]))
  limitBuyersResampling <- 5
  while(resampleBuyers & (sampleBuyersIter<=limitBuyersResampling) & (prodcg[availableForSample==TRUE,.N]>0)){
    new_pc <- rbindlist(lapply(1:n_splits,sample_data,fractionOfSupplierperBuyer=(limitBuyersResampling/model$scenvars$nSuppliersPerBuyer),samplingforSeller=resampleBuyers))
    validIndex <- new_pc[pc[,.(BuyerID,SellerID,Commodity_SCTG)],.I[is.na(NAICS)],on=c("BuyerID","SellerID","Commodity_SCTG")]
    pc <- unique(rbind(pc,new_pc[validIndex])[,Proportion:=NULL])
    rm(new_pc)
    sellersMaxedOut <- pc[,sum(PurchaseAmountTons)>unique(OutputCapacityTons),by=.(SellerID)][V1==TRUE,SellerID]
    buyersOfSellers <- pc[SellerID %in% sellersMaxedOut,unique(BuyerID)]
    # new_buyDifference <- pc[BuyerID%in%buyersOfSellers,sum(OutputCapacityTons)-unique(PurchaseAmountTons),by=BuyerID][,sum(V1)]
    new_sellDifference <- pc[SellerID%in%sellersMaxedOut,sum(PurchaseAmountTons)-unique(OutputCapacityTons),by=SellerID][,sum(V1)]
    resampleBuyers <- (new_sellDifference < (1.10*sellDifference)) # Increased in the maxed out capacity of the sellers should be at least 10%. 
    prodcg[SellerID %in% sellersMaxedOut,availableForSample:=FALSE]
    conscg[!(BuyerID %in% buyersOfSellers), doSample:=FALSE]
    sampleBuyersIter <- sampleBuyersIter + 1
    print(paste0("Number of Combinations: ",pc[,.N]))
  }
  print(paste0("Number of sampling iterations for Buyers: ", sampleBuyersIter-1))




  print(paste(Sys.time(), "Organizing data for distribution channel model"))

  #Distribution size model

  #buyer/seller attributes

  
  pc[, c("emple49", "emp50t199", "empge200", "mfgind", "trwind", "whind") := 0]

  pc[Buyer.Size <= 49, emple49 := 1]

  pc[Buyer.Size >= 50 & Buyer.Size <= 199, emp50t199 := 1]

  pc[Buyer.Size >= 200, empge200 := 1]



  pc[,Seller.NAICS2:=substr(Seller.NAICS,1,2)]

  pc[Seller.NAICS2 %in% 31:33, mfgind := 1]

  pc[Seller.NAICS2 %in% 48:49, trwind := 1]

  pc[Seller.NAICS2 %in% c(42, 48, 49), whind := 1]

  pc[,Buyer.NAICS2:=substr(Buyer.NAICS,1,2)]

  pc[Buyer.NAICS2 %in% 31:33, mfgind := 1]

  pc[Buyer.NAICS2 %in% 48:49, trwind := 1]

  pc[Buyer.NAICS2 %in% c(42, 48, 49), whind := 1]



  pc[, CATEGORY := famesctg[Commodity_SCTG]]



  setkey(pc, Production_zone, Consumption_zone)

  pc <- merge(pc, mesozone_gcd, c("Production_zone", "Consumption_zone")) # append distances

  setnames(pc, "GCD", "Distance")



  print(paste(Sys.time(), "Applying distribution channel model"))

  #Apply choice model of distribution channel and iteratively adjust the ascs

  #The model estimated for mfg products was applied to all other SCTG commodities

  inNumber <- nrow(pc[Commodity_SCTG %in% c(1:9)])

  if (inNumber > 0) {

    setkeyv(pc, c("CATEGORY", unique(distchan_food$VAR[distchan_food$TYPE == "Variable"]))) #sorted on vars, calibration coefficients, so simulated choice is ordered correctly

    pc_food <- pc[Commodity_SCTG %in% c(1:9),c("CATEGORY", unique(distchan_food$VAR[distchan_food$TYPE == "Variable"])),with=F]

    df <- pc_food[, list(Start = min(.I), Fin = max(.I)), by = eval(c("CATEGORY", unique(distchan_food$VAR[distchan_food$TYPE == "Variable"])))] #unique combinations of model coefficients

    df[, eval(unique(distchan_food$VAR[distchan_food$TYPE == "Constant"])) := 1] #add 1s for constants to each group in df

    print(paste(Sys.time(), nrow(df), "unique combinations"))

    pc[Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchan_food, distchan_cal, distchan_calcats, 4)]

  }

  print(paste(Sys.time(), "Finished ", inNumber, " for Commodity_SCTG %in% c(1:9)"))

  outNumber <- nrow(pc[!Commodity_SCTG %in% c(1:9)])

  if (outNumber > 0) {

    setkeyv(pc, c("CATEGORY", unique(distchan_mfg$VAR[distchan_mfg$TYPE == "Variable"]))) #sorted on vars so simulated choice is ordered correctly

    pc_mfg <- pc[!Commodity_SCTG %in% c(1:9),c("CATEGORY", unique(distchan_mfg$VAR[distchan_mfg$TYPE == "Variable"])),with=F]

    df <- pc_mfg[, list(Start = min(.I), Fin = max(.I)), by = eval(c("CATEGORY", unique(distchan_mfg$VAR[distchan_mfg$TYPE == "Variable"])))] #unique combinations of model coefficients

    ####do this in the function (seems unecessary here)?

    df[, eval(unique(distchan_mfg$VAR[distchan_mfg$TYPE == "Constant"])) := 1] #add 1s for constants to each group in df

    print(paste(Sys.time(), nrow(df), "unique combinations"))

    pc[!Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchan_mfg, distchan_cal, distchan_calcats, 4)]

  }

  rm(df)

  print(paste(Sys.time(), "Finished ", outNumber, " for !Commodity_SCTG %in% c(1:9)"))



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
  nShipSizet <- as.numeric(ShipSize[,.N])
  n_splits <- ceiling(npct*nShipSizet*size_per_row/((ram_to_use*(1024**3))/cores_used))
  suppressWarnings(pc[,':='(n_split=1:n_splits,k=1)])
  
  findMinLogisticsCost <- function(split_number){
    pc_split <- pc[n_split==split_number]
    pc_split <- merge(pc_split,ShipSize,by="k",allow.cartesian=TRUE)
    pc_split[,k:=NULL]
    setkey(pc_split, Production_zone, Consumption_zone)
    # print(paste0(object.size(pc_split)/(1024**3)," Gb"))
    df_fin <- minLogisticsCost(pc_split,0, naics, recycle_check_file_path)
    setnames(df_fin,c("time","path","minc"),c("Attribute2_ShipTime","MinPath","MinGmnql"))
    setkey(df_fin,SellerID,BuyerID,NAICS,Commodity_SCTG, weight)
    setkey(pc_split,SellerID,BuyerID,NAICS,Commodity_SCTG, weight)
    return(pc_split[df_fin])
  }

  pc <- rbindlist(lapply(1:n_splits,findMinLogisticsCost))
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

         "lssbd", "MinGmnql", "MinPath") := NULL]



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