buildValidateGraph <- function(model.data, FAF.data, ref.data, 
                               result.measure, geographic.aggr,
                               ref.legend.name, x.label, chart.type = "line") {
  # Builds a ggplot2 graph comparing RSG model, FAF, and some other data source on a national or regional level
  #
  # Args:
  #   model.data     : data.table containing the unaggregated RSG model output to be compared
  #   FAF.data       : data.table containing the unaggregated FAF survey data to be compared
  #   ref. data      : data.table containing the unaggregated reference source data to be compared [OPTIONAL]
  #   result.measure : one of "Tons", "Tonmiles", "Values", "Tonshare", "Tonmileshare", or "Valueshare", 
  #                      determines which measure of comparison will be graphed
  #   geographic.aggr: either "national", "regional", "cmap", or "regional.cmap", determines whether a single nationwide comparison graph
  #                      will be created or a facet graph for each region or CMAP type.
  #   ref.legend.name: a string, the name of the reference data set to be used in the graph [OPTIONAL]
  #   x.label        : a string, the label for the x-axis of the graph
  #   chart.type     : either "line" or "bar", defaults to bar if there is only one commodity type (e.g. T-100 data)
  #
  # Returns:
  #   A ggplot object
  #
  # Other requirements for model.data, FAF.data, and ref.data:
  #   These three data.table must include the following column names: 
  #     "Origin.Region", "Destination.Region", "Commodity.Cat.Code", "Tons", "Tonmiles", "Values"
  #     additionally, if geographic.aggr == "cmap", a "Move.Type" column must be included.
  #     The numeric columns of these data tables must all be of type "numeric", not having any extra classes
  #     Commodity.Cat.Code entries must be short, probably 'numeric" type only. Use an external correspondence
  #       table to clarify the meaning of the numbers.
  
  # Requiring packages
  require(ggplot2)
  require(data.table)
  
  if(missing(ref.data) & missing(ref.legend.name)){
    ref.present <- FALSE
  } else {
    ref.present <- TRUE
  }
  
  # Checking code assumptions
  required.columns <- c("Origin.Region", "Destination.Region", "Commodity.Cat.Code", "Tons", "Tonmiles", "Values")
  if(!(all(required.columns %in% colnames(model.data)))){
    warning("model.data does not contain all the required columns.  ")
  }
  if(!(all(required.columns %in% colnames(FAF.data)))){
    warning("FAF.data does not contain all the required columns")
  }   
  if(!(result.measure %in% c("Tons", "Tonmiles", "Values", "Tonshare", "Tonmileshare", "Valueshare"))){
    warning("Invalid result.measure argument  ")
  }
  if(!(geographic.aggr %in% c("national", "regional", "cmap", "regional.cmap"))){
    warning("Invalid geographic.aggr argument  ")
  }
  if(!(chart.type %in% c("line", "bar"))){
    warning("Invalid chart.type argument  ")
  } 
  if(ref.present){
    if(!(all(c("Origin.Region", "Destination.Region", "Commodity.Cat.Code") %in% colnames(ref.data)))){   
      warning("ref.data does not contain all the required columns.  ")
    }
    if(geographic.aggr == "cmap" & !("Move.Type" %in% colnames(ref.data))){
      warning("geographic.aggr == 'cmap', but the 'Move.Type' column was not found in ref.data.  ")
    }
  }
  
  if(result.measure %in% c("Tons", "Tonshare")){result.type <- "Tons"}
  if(result.measure %in% c("Tonmiles", "Tonmileshare")){result.type <- "Tonmiles"}
  if(result.measure %in% c("Values", "Valueshare")){result.type <- "Values"}
  
  if(ref.present){
    ref.data.measure.present <- result.type %in% colnames(ref.data)
  } else {
    ref.data.measure.present <- FALSE
  }
  
  if (geographic.aggr == "regional" | geographic.aggr == "regional.cmap"){
    # Aggregate regionally by Origin.Region, Destination.Region and Commodity.Cat.Code, using generic column names to be replaced later
    aggregate.model <- model.data[, .(measure.total = sum(get(result.type))), by = .(Commodity.Cat.Code, Origin.Region, Destination.Region)]
    aggregate.model[, measure.share := measure.total/sum(measure.total), by = .(Origin.Region, Destination.Region)]
    
    aggregate.FAF <- FAF.data[, .(measure.total = sum(get(result.type))), by = .(Commodity.Cat.Code, Origin.Region, Destination.Region)]
    aggregate.FAF[, measure.share := measure.total/sum(measure.total), by = .(Origin.Region, Destination.Region)]
    
    if(ref.data.measure.present){
      aggregate.ref <- ref.data[, .(measure.total = sum(get(result.type))), by = .(Commodity.Cat.Code, Origin.Region, Destination.Region)]
      aggregate.ref[, measure.share := measure.total/sum(measure.total), by = .(Origin.Region, Destination.Region)]
    }
  }
  
  if (geographic.aggr == "national") {
    # Aggregate nationally by Commodity.Cat.Code, using generic column names to be replaced later
    aggregate.model <- model.data[, .(measure.total = sum(get(result.type))), by= Commodity.Cat.Code]
    aggregate.model[, measure.share := measure.total/sum(measure.total)]
    
    aggregate.FAF <- FAF.data[, .(measure.total = sum(get(result.type))), by = Commodity.Cat.Code]
    aggregate.FAF[, measure.share := measure.total/sum(measure.total)]
    
    if(ref.data.measure.present){
      aggregate.ref <- ref.data[, .(measure.total = sum(get(result.type))), by = Commodity.Cat.Code]
      aggregate.ref[, measure.share := measure.total/sum(measure.total)]
    }
  }
  
  if(geographic.aggr == "cmap"){
    # Aggregate by to, from, within CMAP by Move.Type and Commodity.Cat.Code, using generic column names to be replaced later
    aggregate.model <- model.data[, .(measure.total = sum(get(result.type))), by = .(Commodity.Cat.Code, Move.Type)]
    aggregate.model[, measure.share := measure.total/sum(measure.total), by = .(Move.Type)]
    
    aggregate.FAF <- FAF.data[, .(measure.total = sum(get(result.type))), by = .(Commodity.Cat.Code, Move.Type)]
    aggregate.FAF[, measure.share := measure.total/sum(measure.total), by = .(Move.Type)]
    
    if(ref.data.measure.present){
      aggregate.ref <- ref.data[, .(measure.total = sum(get(result.type))), by = .(Commodity.Cat.Code, Move.Type)]
      aggregate.ref[, measure.share := measure.total/sum(measure.total), by = .(Move.Type)]
    }
  }
  ##########################################

  ## Replace generic column names and add data source identifiers
  # The string "hare" is appended because "Tons", "Tonmiles", and "Values" can be converted to
  #   "Tonshare", "Tonmileshare", and "Valueshare", respectively, by appending "hare".
  setnames(aggregate.model, c("measure.total", "measure.share"), c(result.type, paste0(result.type, "hare")))
  aggregate.model[, Table := "Model"]
  setorder(aggregate.model, "Commodity.Cat.Code")
  
  setnames(aggregate.FAF, c("measure.total", "measure.share"), c(result.type, paste0(result.type, "hare")))
  aggregate.FAF[, Table := "FAF 2012"]
  setorder(aggregate.FAF, "Commodity.Cat.Code")
  
  if(ref.data.measure.present) {
    setnames(aggregate.ref, c("measure.total", "measure.share"), c(result.type, paste0(result.type, "hare")))
    aggregate.ref[, Table := ref.legend.name]
    setorder(aggregate.ref, "Commodity.Cat.Code")
  }

  #############
  ## Generate plot
  #############
  if(ref.data.measure.present){
    aggregate.all <- rbind(aggregate.model, aggregate.FAF, aggregate.ref)
    color.scale <- c(makeMoreColors(3)[1], makeMoreColors(3)[3], makeMoreColors(3)[2])
  } else {
    aggregate.all <- rbind(aggregate.model, aggregate.FAF)
    color.scale <- c(makeMoreColors(3)[1], makeMoreColors(3)[3])
  }
  
  aggregate.all[, Commodity.Cat.Code := factor(Commodity.Cat.Code)]
  
  # Change facet titles by adding "To" and "From" to aggregate.all columns
  if(geographic.aggr == "regional" | geographic.aggr == "regional.cmap"){
    aggregate.all[, Origin.Region := paste("From", Origin.Region)]
    aggregate.all[, Destination.Region := paste("To", Destination.Region)]
  }
  
  p <- aggregate.all %>% 
    ggplot(aes(x=Commodity.Cat.Code,y=get(result.measure),group=Table,color=Table), environment = environment())+
    theme_db+
    theme(axis.text.x = element_text(angle=(90-60),hjust=1,vjust=1), legend.position = "bottom")+
    labs(color = "Data Source", group = "Data Source", lty = "Data Source", shape = "Data Source", fill = "Data Source")+
    xlab(x.label)+
    scale_color_manual(values = color.scale)+
    scale_fill_manual(values = color.scale)
  
  ## Change y-label to reflect units of result.measure
  if(result.measure == "Tonmiles"){
    p <- p + ylab(paste(result.measure, "(Millions)"))
  } else if(result.measure == "Values"){
    p <- p + ylab(paste("Values", "(Million USD)"))
  } else if(result.measure == "Tons") {
    p <- p + ylab(paste(result.measure, "(Thousands)"))
  } else {
    p <- p + ylab(result.measure)
  }
  
  ## Perform facetting if graph is to compare regions
  if(geographic.aggr == "regional"){
    p <- p + facet_grid(Destination.Region~Origin.Region) #, scales = "free_y"
  }
  
  ## Perform facetting if graph is to compare in, out, within CMAP
  if(geographic.aggr == "cmap"){
    p <- p + facet_wrap(~Move.Type)
  }
  
  if(geographic.aggr == "regional.cmap"){
    p <- p + facet_wrap(~Origin.Region+Destination.Region, nrow = 2) #, scales = "free_y"
  }
  ##############################################################
  
  ## If a line graph is desired, make lines and points mapped to Data Source
  if (chart.type == "line"){
    p <- p + geom_line(aes(lty=Table),lwd=1)+
         geom_point(aes(shape=Table), size = 3)
  }
  
  ## If a bar graph is desired, make bars mapped to Data Source
  if (chart.type == "bar"){
    p <- p + geom_bar(aes(fill=Table),stat="identity",position="dodge")
  }
  
  return(p)
}