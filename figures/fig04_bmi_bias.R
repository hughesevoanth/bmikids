# File Metadata ################################################################
# Title: Figure 4 — height-dependent bias in BMI during growth (analytic)
# Author: David A Hughes
# Email: david.hughes@pbrc.edu
# Date: 20 Nov 2026
# Version: 2.0
#
# Input : data/derived/res_fig4.rds (03_kids.Rmd; β from the sex-adjusted Table 2 values)
# Output: results/source_data/SourceData_Fig4.csv; results/figures/Figure4.pdf / .png
#
# bias = (H / H0)^(β − 2) − 1, with H0 the age-specific median height.

source(if (file.exists("R/setup.R")) "R/setup.R" else "../R/setup.R")

src <- read_result("res_fig4") |>
  transmute(stage, beta, label, x = rel_height, y = bias_pct, element = "line_vertex")
src <- write_source_data(src, 4)

stage_order <- unique(src$label)                     # adult-like, age 3, age 5, age 11
src <- src |> mutate(label = factor(label, levels = stage_order))

fig4 <- ggplot(src, aes(x, y, colour = label, linetype = label, linewidth = label)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_vline(xintercept = 1, linetype = "dashed") +
  geom_line() +
  scale_x_continuous(breaks = seq(0.90, 1.10, by = 0.05), labels = scales::label_percent(accuracy = 1)) +
  scale_y_continuous(breaks = seq(-15, 15, 5), labels = lab_minus) +
  scale_colour_manual(values    = set_names(c("grey70", "grey45", "grey25", "black"), stage_order)) +
  scale_linetype_manual(values  = set_names(c("solid", "dotted", "dashed", "solid"), stage_order)) +
  scale_linewidth_manual(values = set_names(c(1, 1.2, 1.2, 1.6), stage_order)) +
  labs(x = "Height relative to age-specific median",
       y = "BMI bias (%)\nassuming equal true relative weight",
       colour = "Growth stage", linetype = "Growth stage", linewidth = "Growth stage") +
  theme_bmikids()

save_figure(fig4, "Figure4", width = 7, height = 4.5)

sessioninfo::session_info()
