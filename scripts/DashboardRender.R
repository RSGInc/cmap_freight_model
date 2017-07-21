DashboardRender <- function(scenarios, include.ref = TRUE, display.timing = FALSE) {
  # Builds CMAP dashboard to the appropriate location within the scenarios
  # folder of the model file structure.
  #
  # Args:
  #   scenarios: vector of strings, with each string being the name of a
  #                scenario folder within the scenarios folder of the rFreight model file
  #                structure.
  #   include.ref: boolean, if TRUE, FAF and other reference datasets
  #                  will be included in the final dashboard. If FALSE, these
  #                  datasets will not be included and some charts and tables
  #                  will come up blank.
  #   display.timing: boolean, if TRUE, timing information will be printed to the
  #                     console after the dashboard is rendered. If FALSE, no
  #                     such information will be printed.
  #
  # Returns:
  #   None, but creates a .html file of the dashboard. If length(scenarios) == 1,
  #     the .html file is placed in the outputs file for that specific scenario.
  #     If length(scenarios) > 1, then the file is placed in the scenarios folder
  #     on the same level as the folders for each specific scenario.
  
  if (display.timing) {run.time <- c(Sys.time())}
  
  if (length(scenarios) < 1 | is.na(scenarios)) {
    # You need at least one model run listed to run the dashboard
    warning("Please include at least one model run in the dashboard.")
  } else if (length(scenarios) > 3) {
    # More than 3 scenarios would make dashboard graphs very cluttered
    warning("The dashboard is best viewed with 1, 2, or 3 model runs. More than three model runs will make the charts very cluttered. Please reduce the number of model runs given.")
  } else if (length(scenarios) == 1) {
    # Comparison of a single scenario against FAF and references
    rmarkdown::render(input = "./Dashboard/CMAPDashboard.Rmd",
                      output_file = paste0("CMAPDashboard_", scenarios[1], ".html"),
                      output_dir = paste0("./scenarios/", scenarios[1], "/outputs"),
                      params = list(model.run.names = scenarios,
                                    include.reference = include.ref))
  } else if (length(scenarios) == 2) {
    # Scenario comparisons of 2 scenarios against FAF and references
    rmarkdown::render(input = "./Dashboard/CMAPDashboard.Rmd",
                      output_file = paste0("CMAPDashboard_", scenarios[1], "_", scenarios[2], ".html"),
                      output_dir = paste0("./scenarios"),
                      params = list(model.run.names = scenarios,
                                    include.reference = include.ref))
  } else if (length(scenarios) == 3) {
    # Scenario comparisons of 3 scenarios against FAF and references
    rmarkdown::render(input = "./Dashboard/CMAPDashboard.Rmd",
                      output_file = paste0("CMAPDashboard_", scenarios[1], "_", scenarios[2], "_", scenarios[3], ".html"),
                      output_dir = paste0("./scenarios"),
                      params = list(model.run.names = scenarios,
                                    include.reference = include.ref))
  }
  
  if (display.timing) {
    run.time <- c(run.time, Sys.time())
    cat("Dashboard Compilation Diagnostics", "\n")
    cat(paste("Number of Scenarios:", length(scenarios)), "\n")
    run.time <- setNames(run.time, c("Start Time", "End Time"))
    cat(run.time, "\n")
  }
}

### In-model automatic dashboard creation ----------
args <- commandArgs(trailingOnly = TRUE)
if (!is.na(args[1])) {
  model.scenario <- c(args[1])
  DashboardRender(scenarios = model.scenario)
}
rm(args)