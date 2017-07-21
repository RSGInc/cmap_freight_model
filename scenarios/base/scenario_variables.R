##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       scenario_variables.R creates variables for use in the model, grouped by
#                   model step. The variables are specific to a scenario
#Date:              January 28, 2014
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.
##############################################################################################

#---------------------------------------------------------------------
#Declare variables used in the model
#---------------------------------------------------------------------
print("Declaring variables used in the model")

#---------------------------------------------------------------------
#Step 0 Overall Model Flow and Control Variables
#---------------------------------------------------------------------

#These flags determine whether various outputs are saved
#Set to TRUE to save output
outputtable       <- FALSE #large output tabulations
outputsummary     <- TRUE #summary output files
outputRworkspace  <- FALSE #if TRUE save workspace with all files at the end of each model step
outputlog         <- FALSE #sink output and messages to log.txt
outputprofile     <- FALSE #profiling (set to FALSE for production)

#---------------------------------------------------------------------
#Step 1 Firm Synthesis
#---------------------------------------------------------------------

provalthreshold       <- 0.8     # threshold for percentage of purchase value for each commodity group met by producers
combinationthreshold  <- 3500000 # max number of combinations of producers and consumers to enter into a procurement market game
consprodratiolimit    <- 1000000     # limit on ratio of consumers to producers to enter into the procurement market game
foreignprodcostfactor <- 0.9     # producer cost factor for foreign produers (applied to unit costs)
wholesalecostfactor    <- 1.2     # markup factor for wholesalers (applied to unit costs)

#---------------------------------------------------------------------
#Step 2 Mode-Path Paramaters
#---------------------------------------------------------------------

#Path parameters: assume the following parameters for all alternatives
B1                 <- 100            #Constant unit per order
B4                 <- 2000           #Storage costs per unit per year
j                  <- 0.01           #Fraction of shipment that is lost/damaged
LT_OrderTime       <- 10             #Expected lead time for order fulfillment (to be added to in-transit time)
sdLT               <- 1              #Standard deviation in lead time

#Path parameters: specific costs for mode specific rates and handlings fees
BulkHandFee        <- 1              #Handling charge for bulk goods ($ per ton)
WDCHandFee         <- 15             #Warehouse/DC handling charge ($ per ton)
IMXHandFee         <- 15             #Intermodal lift charge ($ per ton)
TloadHandFee       <- 10             #Transload charge ($ per ton; at international ports only)
AirHandFee         <- 20             #Air cargo handling charge ($ per ton)
WaterRate          <- 0.005          #Line-haul charge, water ($ per ton-mile)
CarloadRate        <- 0.03           #Line-haul charge, carload ($ per ton-mile)
IMXRate            <- 0.04           #Line-haul charge, intermodal ($ per ton-mile)
AirRate            <- 3.75           #Line-haul charge, air ($ per ton-mile)
LTL53rate          <- 0.08           #Line-haul charge, 53 feet LTL ($ per ton-mile)
FTL53rate          <- 0.08           #Line-haul charge, 53 feet FTL ($ per ton-mile)
LTL40rate          <- 0.1            #Line-haul charge, 40 feet LTL ($ per ton-mile)
FTL40rate          <- 0.1            #Line-haul charge, 40 feet FTL ($ per ton-mile)
ExpressSurcharge   <- 1.5            #Surcharge for direct/express transport (factor)
BulkTime           <- 72             #Handling time at bulk handling facilities (hours)
WDCTime            <- 12             #Handling time at warehouse/DCs (h⌠ours)
IMXTime            <- 24             #Handling time at intermodal yards (hours)
TloadTime          <- 12             #Handling time at transload facilities (hours)
AirTime            <- 12              #Handling time at air terminals (hours)

#Allow discount rate to vary by type of good.
#Assume discount rate (B2, B3, B5)) is same for goods in transit (B3) + goods in storage (B5)
LowDiscRate       <- 0.01
MedDiscRate       <- 0.05
HighDiscRate      <- 0.25

#Mode availability parameters based on shipment size vs. vehicle capacity
CAP1FTL           <- 30*2000        #Capacity of 1 Full Truckload = 30 tons
CAP1Carload       <- 100000	        #Capacity of 1 Full Railcar = 85 tons (85*2000); Heither, 10-05-2016: test 100000 lbs so mode is actually used
CAP1Airplane      <- 25*2000        #Capacity of 1 Airplane Cargo Hold = 25 ton


#Allow "a" to vary by type of commodity (Functional vs. Innovative)
#Assume "a" is the same for goods inside three different groups
LowMultiplier      <- 0.5
MediumMultiplier   <- 1.0
HighMultiplier     <- 2.33

#Allow "sdQ" to vary by type of commodity (Functional vs. Innovative)
#Assume "sdQ" is the same for goods inside three different groups
LowVariability      <- 0.03
MediumVariability   <- 0.06
HighVariability     <- 0.09

# Supplier Sampling variables
distBased <- FALSE #Whether to use distance based distribution or distance-ton based distribution
nSuppliersPerBuyer <- 20
distance_bins <- seq(0,150000,100)


#---------------------------------------------------------------------
#Step 3 Run PMG
#---------------------------------------------------------------------

# max number of R script instances to run at once for determining Supplier to Buyer costs (1 for monitoring if that is run)
maxcostrscripts <- min(16, max(1, future::availableCores()-1))

# max number of R script instances to run at once (1 for monitoring if that is run, the rest for running PMGs)
maxrscriptinstances <- min(16, max(1, future::availableCores()-1))
#maxrscriptinstances <- maxcostrscripts


availableRAM <- 120*(1024**3) # 120 Gb
# Max allocation of memory to each future instance
futureMaxSize <- max(15*(1024**3),availableRAM/max(maxcostrscripts,(maxrscriptinstances-1))) # At least 10 Gb per future.

# should monitoring be run?
pmgmonitoring <- TRUE

# Full NAICS run
pmgnaicstorun <- c("113000","114000",
"211000","212100","212230","2122A0","212310","2123A0",
"311111","311119","311210","311221","311225","31122A","311300","311410","311420","311513","311514","31151A","311520","311615","31161A","311700","311810","3118A0","311910","311920","311930","311940","311990","312110","312120","312130","312140","312200","313100","313200","313300","314120","314900","315000","316000",
"321100","321200","321910","3219A0","322110","322120","322130","322210","322220","322230","322291","322299","323110","323120","324110","324121","324122","324190","325110","325120","325130","325180","325190","325211","3252A0","325310","325320","325411","325412","325413","325414","325510","325520","325610","325620","325910","3259A0","326110","326120","326130","326140","326150","326160","326190","326210","326220","326290","327100","327200","327310","327320","327330","327390","327400","327910","327991","327992","327993","327999",
"331110","331200","33131A","33131B","331411","331419","331420","331490","331510","331520","332114","33211A","33211B","332200","332310","332320","332410","332420","332430","332500","332600","332710","332720","332800","332913","33291A","332991","332996","33299A","33299B","333111","333112","333120","333130","333220","333295","33329A","333313","333314","333315","33331A","333414","333415","33341A","333511","333514","33351A","33351B","333611","333612","333613","333618","333912","33391A","333920","333993","333994","33399A","33399B","334111","334112","33411A","334210","334220","334290","334300","334413","334418","33441A","334510","334511","334512","334513","334514","334515","334516","334517","33451A","334610","335110","335120","335210","335221","335222","335224","335228","335311","335312","335313","335314","335911","335912","335920","335930","335991","335999","336120","336211","336214","336310","336320","336350","336360","336370","336390","3363A0","336411","336412","336413","336414","33641A","336500","336611","336612","336991","336992","336999","337110","337121","337122","337127","33712A","337215","337900","339112","339113","339114","339116","339910","339920","339930","339940","339950","339990",
"511110","511120","511130","5111A0")

# Partial NAICS run
# pmgnaicstorun <- c(
  # "2122A0"
 # 326220, 212230,336414, 332420
#   324122,327400,339910,327310,327200,327993,327992,327999,327991,327100,327330,
#   113000, 211000,212100,
#   "2123A0","312200"
  # 327100, 327390, 327200, 324190, 211000, 324110, 327310, 327992, 339910
# )

# NAICS group for Sensitivity Analysis
# pmgnaicstorun <- c("212100","3219A0","333920","336120","336214","336310","336350","336360","336370",
#                    "336390","3363A0","336413","33641A","336500","336992","336999")

#pmgnaicstorun <- c("212100","2122A0","339910")#,"3219A0","333920","336120","336214","336310","336350","336360","336370",
                  # "336390","3363A0","336413","33641A","336500","336992","336999")
# pmgnaicstorun <- c("211000","2122A0","2123A0","327100","327200","327992","339910")

#monitoring settings
# pmgmonfrom <- "cheither@cmap.illinois.gov"
# pmgmonto <- c("cheither@cmap.illinois.gov","lcruise@cmap.illinois.gov")
# pmgmonsmtp <- "outlook.office365.com"
# pmgmoninterval <- 43200
pmgmonfrom <- "colin.smith@rsginc.com"
pmgmonto <- "colin.smith@rsginc.com"
pmgmonsmtp <- "WRJHUBVPW01.i-rsg.com"
pmgmoninterval <- 14400

# should pmg logging be on? (see also verbose below for level of detail)
pmglogging <- TRUE

# random starting seed for the PMGs
RandomSeed <- 41

# number of iterations
IMax <- 6 # Default was 6

# want lots of detail about tradebots?
Verbose <- 0 # 0:FALSE	1:TRUE

# recalculate alternate payoffs every iteration based on updated expected payoffs
DynamicAlternatePayoffs <- 1

# should initial expected tradeoffs know size of other traders?
ClairvoyantInitialExpectedPayoffs <- 0

# should sellers accept offers based on order size instead of expected payoff?
SellersRankOffersByOrderSize <- 0

# multiplier to goose initial expected tradeoff to encourage experimentation with other traders
InitExpPayoff <- 1.1
Temptation    <- 0.6
BothCoop      <- 1.0
BothDefect    <- 0.6
Sucker        <- 1.0

# amount to downgrade expected payoff of seller who outright refuses a trade offer by buyer
RefusalPayoff <- 0.5

# negative payoff to sellers for not participating
WallflowerPayoff <- 0.0

#buyers don't try to trade with sold out sellers
BuyersIgnoreSoldOutSellers <- 1

#ratio at which buyers don't try to trade with sold out sellers
IgnoreSoldOutSellersMinBuyerSellerRatio = 100

#faster reading of input files but does less checks (ok for use with R)
RawFastParser = 1

#---------------------------------------------------------------------
#Step 7 Vehicle Tour Choice Pattern
#---------------------------------------------------------------------
#Based on Illinois weight plates (March 2013) the GVW for truck types are:
#light duty (plates D-J): 28000 lbs , medium duty (plates K-T): 64000 lbs. , heavy duty (plates V-Z): 80000 lbs.
#For simplicity and to allow some wiggle room, assume maximum load weights 0f 35000, 65000, 100000 lbs. (ignore weight of truck)
wgtmax_2axl <- 35000
wgtmax_3axl <- 65000
wgtmax_semi <- 100000




#---------------------------------------------------------------------
#Step 11 Prepare trip table
#---------------------------------------------------------------------
annualfactor        <- 310  #sampling factor to convert annual truck flows to daily

#---------------------------------------------------------------------
#Step NA Run Sensitivity analysis
#---------------------------------------------------------------------
runSensitivityAnalysis <- FALSE
runParameters <- FALSE


#--------------------------------------------------------------------
# Run Mode
#--------------------------------------------------------------------
ApplicationMode <- TRUE
distchannelCalibration <- FALSE & (!ApplicationMode)
modechoiceCalibration <- FALSE & (!ApplicationMode) & (!distchannelCalibration)
