# ---- File Metadata ----
# Title: Evaluate a model on a holdout set
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 29 Aug 2025
# Version: 1.0

#' Extracts and prints the formula with estimated coefficients from a fitted linear model.
#'
#' This function takes a linear model object (e.g., from lm()) and
#' extracts the formula, including the estimated beta coefficients, and prints it
#' to the console as a single predictive equation.
#'
#' @param model A fitted linear model object.
#'
#' @return The function returns the full predictive formula as a character string,
#'   but its primary purpose is to print it to the console.
#'
#' @examples
#' # Example with a simple linear model
#' set.seed(42)
#' df <- data.frame(
#'   y = rnorm(100),
#'   x1 = rnorm(100),
#'   x2 = rnorm(100)
#' )
#' simple_model <- lm(y ~ x1 + x2, data = df)
#' extract_formula(simple_model)
#'
#' # Example with a polynomial term
#' poly_model <- lm(y ~ x1 + I(x2^2), data = df)
#' extract_formula(poly_model)
extract_formula <- function(model) {
  
  # Step 1: Extract the outcome variable name.
  outcome_var <- all.vars(formula(model))[1]
  
  # Step 2: Extract the coefficients from the model.
  betas <- coef(model)
  
  # Step 3: Initialize the formula string with the outcome variable.
  formula_string <- paste(outcome_var, "~")
  
  # Step 4: Add the intercept if it exists.
  if ("(Intercept)" %in% names(betas)) {
    intercept_beta <- round(betas["(Intercept)"], 4)
    formula_string <- paste(formula_string, intercept_beta)
  }
  
  # Step 5: Add the other predictors and their betas.
  # We loop through all betas except the intercept.
  non_intercept_betas <- betas[!names(betas) %in% c("(Intercept)")]
  
  for (i in seq_along(non_intercept_betas)) {
    beta <- round(non_intercept_betas[i], 4)
    var_name <- names(non_intercept_betas)[i]
    
    # Check for negative coefficients to add " - " instead of " + - "
    sign_string <- " + "
    if (beta < 0) {
      sign_string <- " - "
      beta <- abs(beta) # Use absolute value for negative betas
    }
    
    formula_string <- paste(formula_string, sign_string, beta, "*", var_name)
  }
  
  # Step 6: Print the full formula string to the console.
  # cat("The formula used to predict", outcome_var, "is:\n")
  cat(formula_string, "\n")
  
  # Return the formula string invisibly for programmatic use.
  invisible(formula_string)
}
