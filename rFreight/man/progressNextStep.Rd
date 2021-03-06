% Generated by roxygen2 (4.1.1): do not edit by hand
% Please edit documentation in R/progressNextStep.R
\name{progressNextStep}
\alias{progressNextStep}
\title{Increments the progress bar to the next progress step}
\usage{
progressNextStep(stepname)
}
\arguments{
\item{stepname}{Name of the step within a model component about to be run, character string}
}
\description{
This function is called at the beginning of a step within a model component. It
increments the progress bar and change the text describing the step now taking place.
It also calculate the run time for the preceding step.
}
\examples{
\dontrun{
progressNextStep("Reading Inputs")
}
}
\keyword{Management}

