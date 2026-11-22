# File Metadata ################################################################
# Title: Figure 1 — adult weight–height allometry (log–log and original scale)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig1.rds (02_adults.Rmd), data/derived/adults.rds (points only)
# Output: results/source_data/SourceData_Fig1.csv
#         results/figures/Figure1.pdf / .png
#
# Source data are aggregate: the fitted lines plus counts in a fine 2-D grid. The individual
# points (coloured by local density) are drawn from adults.rds, which anyone can rebuild
# from public NHANES files with 00_nhanes_fetch_data.R and 01_data_wrangling.R.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")
library(ggpointdensity)

res    <- read_result("res_fig1")
adults <- read_result("adults")

## source data #################################################################
src <- bind_rows(
  res$lines |> transmute(panel = "A", element = "fitted_line", sex,
                         x = log10_height, y = log10_weight),
  res$lines |> transmute(panel = "B", element = "fitted_line", sex,
                         x = height_m, y = weight_kg),
  res$bins  |> transmute(panel, element = "bin_count", sex = NA_character_, x, y, n)
) |>
  mutate(x_variable = if_else(panel == "A", "log10 height (m)", "height (m)"),
         y_variable = if_else(panel == "A", "log10 weight (kg)", "weight (kg)"),
         .after = panel)

src <- write_source_data(src, 1)
lines <- src |> filter(element == "fitted_line") |>
  mutate(sex = factor(sex, levels = names(pal_sex)[1:2]))

## plotting window #############################################################
# Everyone stays in the model, the density estimate and the source data; the axes are
# zoomed (coord_cartesian) so that a single extreme value does not compress the panels.
y_max_kg <- 250
n_off    <- sum(adults$weight_kg > y_max_kg)
cli::cli_alert_info("{n_off} participant{?s} above {y_max_kg} kg not shown (included in all estimates)")

## shared pieces ###############################################################
#' One allometry panel: density-coloured points plus sex-specific fitted lines
allometry_panel <- function(xvar, yvar, panel_id, label, label_x, label_y,
                            x_title, y_title) {
  ggplot(adults, aes(.data[[xvar]], .data[[yvar]])) +
    geom_pointdensity(size = 0.5, adjust = 0.5, alpha = 0.5) +
    scale_colour_viridis_c(guide = "none") +
    ggnewscale::new_scale_colour() +
    geom_line(data = filter(lines, panel == panel_id),
              aes(x, y, colour = sex, linetype = sex), linewidth = 0.9) +
    scale_colour_manual(values = pal_sex, name = "Sex") +
    scale_linetype_manual(values = c(Females = "solid", Males = "dashed"), name = "Sex") +
    annotate("richtext", x = label_x, y = label_y, label = paste0("<b>", label, "</b>"),
             size = 3, hjust = 0, fill = NA, label.colour = NA) +
    labs(x = x_title, y = y_title) +
    theme_bmikids() +
    theme(axis.title.x = element_markdown(), axis.title.y = element_markdown())
}

## Figure 1A: log–log model ####################################################
pA <- allometry_panel("log10_height", "log10_weight", "A", res$labels$A,
                      label_x = min(adults$log10_height), label_y = min(adults$log10_weight),
                      x_title = "<b>log<sub>10</sub> height (m)</b>",
                      y_title = "<b>log<sub>10</sub> weight (kg)</b>") +
  coord_cartesian(ylim = c(NA, log10(y_max_kg)))

## Figure 1B: back-transformed model ###########################################
pB <- allometry_panel("height_m", "weight_kg", "B", res$labels$B,
                      label_x = min(adults$height_m), label_y = 5,
                      x_title = "<b>height (m)</b>", y_title = "<b>weight (kg)</b>") +
  coord_cartesian(ylim = c(0, y_max_kg))

fig1 <- (pA / pB) +
  plot_layout(guides = "collect") +
  plot_annotation(tag_levels = "A")

save_figure(fig1, "Figure1", width = 6, height = 9)

sessioninfo::session_info()
