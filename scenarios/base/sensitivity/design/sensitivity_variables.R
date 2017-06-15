# This file is to design experiments to perform sensitivity analysis of the CMAP model
require(data.table)
design_environment <- new.env(parent = .GlobalEnv)
with(design_environment,{
  design_file <- file.path(model$basedir,"scenarios","base","sensitivity","design","Design1.ngd")
  suppressWarnings(desing_txt <- readLines(design_file))
  lineEnd <- which(grepl("\\|",desing_txt))
  exp_design <- fread(design_file,nrows=(lineEnd-2))
  exp_design[,ncol(exp_design):=NULL]
  old_names <- c(paste0("a.attr",1:6))
  new_names <- c(paste0("B",0:5))
  setnames(exp_design,old_names,new_names)
  maxLevels <- exp_design[,lapply(.SD,max),.SDcols=new_names]
  B0_values <- seq(0,10000,length.out = maxLevels[,B0])	# Constant
  B1_values <- seq(100,500,length.out = maxLevels[,B1])		# Constant unit per order
  B2_multiplier <- seq(0.5,1.5,length.out = maxLevels[,B2])
  B3_multiplier <- seq(0.5,1.5,length.out = maxLevels[,B3])
  B4_values <- seq(1000,5000,length.out = maxLevels[,B4])	# Storage costs per unit per year
  B5_multiplier <- seq(0.5,1.5,length.out = maxLevels[,B5])
  exp_design[,paste0("B",c(0:5),"_values") := .(B0_values[B0], B1_values[B1], B2_multiplier[B2], B3_multiplier[B3], B4_values[B4], B5_multiplier[B5])]
  setnames(exp_design,"Choice situation","Choice")
  assign("exp_design",exp_design,envir = .GlobalEnv)
  }
)
rm(design_environment)
