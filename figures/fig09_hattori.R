# File Metadata ################################################################
# Title: Figure 9 — Hattori chart: mean FFMI vs FMI across the life course, by sex
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig9.rds (04_kids_and_adults.Rmd)
# Output: results/source_data/SourceData_Fig9.csv; results/figures/Figure9.pdf / .png
#
# Because FFMI + FMI = BMI, lines of slope −1 are constant BMI (solid grey) and rays through
# the origin are constant %BF (dotted grey). Youth in single years (8–20 y), adults in 5-year
# bands (21–24 … 80+); arrows mark the oldest stratum of each cohort.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

src <- read_result("res_fig9") |>
  transmute(cohort, sex = as.character(sex), stratum, n, age_mid,
            x = ffmi, y = fmi, bmi, pbf, element = "stratum_mean")
src <- write_source_data(src, 9) |>
  arrange(cohort, sex, age_mid)

## reference lines and their labels ############################################
iso_bmi <- c(18.5, 20, 25, 30)
iso_pf  <- seq(20, 45, by = 5)
win     <- list(x0 = floor(min(src$x)) - 0.5, x1 = ceiling(max(src$x)) + 0.5,
                y0 = floor(min(src$y)) - 0.5, y1 = ceiling(max(src$y)) + 0.5)

pf_lab <- tibble(pf = iso_pf, slope = pf / (100 - pf)) |>
  mutate(x = pmin(win$x1 - 0.1, (win$y1 - 0.4) / slope), y = x * slope, lab = paste0(pf, "% fat")) |>
  filter(x > win$x0 + 1)                                    # rays that leave the window early are unlabelled
bmi_lab <- tibble(bmi = iso_bmi) |>
  mutate(x = pmax(win$x0, bmi - (win$y1 - 0.6)) + 0.2,     # where the line enters the window
         y = bmi - x + 0.3,                                 # just above its line
         lab = paste("BMI", bmi)) |>
  filter(y > win$y0, y < win$y1)

fig9 <- ggplot(src, aes(x, y, colour = cohort)) +
  geom_abline(intercept = iso_bmi, slope = -1, colour = "grey40") +
  geom_abline(intercept = 0, slope = iso_pf / (100 - iso_pf), colour = "grey40",
              linetype = "dotted", linewidth = 0.75) +
  geom_path(aes(group = cohort), arrow = arrow(length = unit(2.5, "mm"), type = "closed"), linewidth = 1) +
  geom_point(size = 2.5) +
  ggrepel::geom_text_repel(aes(label = stratum), size = 2.6, seed = 1, nudge_x = 0.3,
                           fontface = "bold", show.legend = FALSE) +
  geom_text(data = bmi_lab, aes(x, y, label = lab), colour = "grey30", size = 2.6, hjust = 0,
            fontface = "bold", inherit.aes = FALSE) +
  geom_text(data = pf_lab, aes(x, y, label = lab), colour = "grey30", size = 2.6, hjust = 1,
            vjust = 1.4, fontface = "bold", inherit.aes = FALSE) +
  facet_wrap(~ sex) +
  scale_colour_manual(values = pal_cohort, name = NULL) +
  coord_equal(xlim = c(win$x0, win$x1), ylim = c(win$y0, win$y1), expand = FALSE) +
  labs(x = "Fat-free mass index, FFMI (kg/m²)", y = "Fat mass index, FMI (kg/m²)") +
  theme_bmikids(base_size = 13) +
  theme(legend.position = "bottom")

save_figure(fig9, "Figure9", width = 10, height = 5.8)

sessioninfo::session_info()
