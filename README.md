# BMI in Kids Review: analysis code
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![R ≥ 4.6](https://img.shields.io/badge/R-%E2%89%A5%204.6-276DC3?logo=r)
[![renv](https://img.shields.io/badge/packages-renv.lock-blue)](renv.lock)

Code to reproduce every figure, Table 2, every number quoted in the text, and the figure
source data for:

> Rodriguez C, Hughes DA, Devarakonda SLS, Yang S, Thomas DM, Pietrobelli A, Heymsfield SB.
> **Body Mass Index: Mathematical Foundation of the World's Most Widely Used Adiposity Index in Pediatrics.**
> *(journal, year, DOI to be added)*

Code: David A Hughes (david.hughes@pbrc.edu), Pennington Biomedical Research Center.

All data are public: NHANES 1999–2023 public-use files (CDC/NCHS) and the CDC 2022 extended
BMI-for-age LMS table. The scripts download both.

---

## Quick start

```r
# 1. Clone the repository and open bmikids.Rproj in RStudio
#    (or start R with the repository root as the working directory).

# 2. Install the exact package versions used for the paper
install.packages("renv")
renv::restore()

# 3. Run everything (downloads ~200 MB of NHANES data on the first run)
source("run_all.R")          # or, in a terminal:  Rscript run_all.R
```

A full run takes about 3 minutes on a laptop once the data are downloaded. Outputs go to
`results/`, and the knitted analysis reports go to `reports/`.

**Requirements:** R ≥ 4.6 and pandoc (bundled with RStudio and Quarto) to knit the reports.
If R cannot find pandoc outside RStudio, set `RSTUDIO_PANDOC` to the folder that contains
it (see *Configuration*).

---

## What the pipeline does

Run the numbered scripts in order (or `run_all.R` for everything). `R/setup.R` is not run on
its own: every script loads it (packages, paths, options, plot theme, shared functions).

| Step | Script | Produces |
|---|---|---|
| 0 | `00_nhanes_fetch_data.R` | **One-time download** of 32 NHANES `.xpt` files (DEMO, BMX, DXX) and the CDC LMS table. Skips files already on disk and logs MD5 checksums; not needed if the data are already present |
| 1 | `01_data_wrangling.R` | Reads, QCs and merges the NHANES files and derives every analysis variable → `data/derived/{nhanes_all, adults, kids, kids_dxa}.rds`, `data_dictionary.csv`, `results/stats/sample_sizes.csv` |
| 2 | `02_adults.Rmd` | Adult analyses (Figure 1 model, adult strata for Figures 2 and 8, in-text numbers) |
| 3 | `03_kids.Rmd` | Youth analyses (Table 2; Figures 3–7, 10, 11; youth strata for Figures 2 and 8; CDC z-score example) |
| 4 | `04_kids_and_adults.Rmd` | Analyses spanning both cohorts (Figure 2, drift comparison for Figure 8C, Hattori chart for Figure 9) |
| 5 | `tables/tab02_beta_by_age.R` | `results/tables/Table2.{csv,docx}` and a cell-by-cell comparison with the manuscript |
| 6 | `figures/figNN_*.R` | `results/figures/FigureN.pdf` (vector) and `.png` (600 dpi), and `results/source_data/SourceData_FigN.csv` |
| 7 | `figures/make_source_data_workbook.R` | `results/source_data/SourceData_AllFigures.xlsx` (README sheet + one sheet per figure) |
| 8 | `R/check_in_text_stats.R` | `results/stats/in_text_stats.csv` (and `.xlsx`, mismatches highlighted): every number quoted in the manuscript next to the value the code reproduces |
| 9 | `R/check_sample_sizes.R` | `results/stats/sample_size_check.csv`: recounts the samples from the raw NHANES files with independent code and checks that every figure and table uses the right one (all participants, or the DXA subset); stops if any count disagrees |

`run_all.R` runs steps 0–9 in order, each in a fresh R session, and writes
`results/stats/session_info.txt`. Every script also ends with `sessioninfo::session_info()`.

**Each figure is drawn from its own source-data file.** Every figure script computes the
plotted values, writes `SourceData_FigN.csv`, reads that file back, and draws the figure
from it. The published source data are therefore exactly what is plotted.

---

## Data

### NHANES files

| Component | Cycles | Variables used |
|---|---|---|
| Demographics (DEMO) | 1999–2000 … 2015–2016, 2017–March 2020 (pre-pandemic, `P_DEMO`), 2021–2023 | SEQN, RIAGENDR, RIDAGEYR, RIDRETH1 |
| Body measures (BMX) | same | BMXWT, BMXHT, BMXWAIST, and comment codes BMIWT, BMIHT |
| Whole-body DXA (DXX) | 1999–2006, 2011–2016 (2017–2018 optional, see below) | DXAEXSTS, DXDTOFAT, DXDTOLE, DXDTOBMC |

- **Whole-body DXA exists for 1999–2006 and 2011–2018 only.** CDC has no whole-body DXA file
  for 2007–2010, the 2017–March 2020 pre-pandemic release or 2021–2023. The ages scanned also
  varied: 8–85 y in 1999–2004, 8–69 y in 2005–2006, and 8–59 y in 2011–2018. Adult DXA at
  60–69 y therefore comes from 1999–2006 only, and at 70+ y from 1999–2004 only.
- **In short:** analyses that need only height and weight use NHANES 1999–2023 (11 cycles);
  DXA analyses use NHANES 1999–2006 and 2011–2016 (7 cycles).
- **2017–2018.** The pre-pandemic 2017–March 2020 files (`P_*`) use new SEQNs, so the
  2017–2018 DXA file (`DXX_J`) cannot be linked to them. By default the pipeline uses the
  `P_*` files, which reproduces the manuscript. Set `BMIKIDS_USE_2017_2018=TRUE` to use
  the 2017–2018 cycle instead: this recovers about 3,600 DXA scans but drops the
  2019–March 2020 participants.
- 1999–2006 DXA files are multiply imputed (5 rows per participant); the pipeline averages
  the implicates.

### Inclusion and QC (`01_data_wrangling.R`)

- Valid standing height and weight, and no weight or height comment code. Missing waist
  circumference does **not** exclude anyone.
- DXA: scan status `DXAEXSTS == 1` and no missing component.
- Percent body fat (`pbf`) = 100 × fat / (fat + lean + bone mineral). It agrees with the
  NHANES-supplied `DXDTOPF` to r = 0.99999.
- Adults are ≥ 21 y; youth are 2–20 y; DXA analyses in youth use ages 8–20.
- **No survey weights.** All estimates are unweighted, as stated in the manuscript.

---

## Main analysis choices

- **Allometric exponent β:** the slope of log₁₀W on log₁₀H within each age stratum (single
  years 2–20; adult decades 21–29 … 70+). It is fit by sex, and also with sex as a covariate
  (sex-adjusted). Strata with n < 100 are skipped.
- **Height–%BF association:** Pearson r by age and sex (Figure 5A), and a sex-adjusted partial
  correlation (Figure 2B: height and %BF each residualised on sex, Fisher-z CI with n − 4).
- **Compositional stability:** `gam(pbf ~ s(bmi, k = 5))` within each stratum, drawn
  between the stratum's 5th and 95th BMI percentiles. %BF is predicted at exactly the
  reference BMI, and only when that BMI lies within 0.5 kg/m² of the stratum's range. The
  sex-adjusted version adds `+ sex` and averages the female and male predictions 50/50.
- **Drift:** the unweighted OLS slope of %BF-at-reference-BMI on stratum age, for reference
  BMIs 20–35 (adults at decade midpoints, with "70+" at 75; youth at single years).
- **Smooths:** thin-plate GAMs (`bs = "tp"`) throughout.
- **Asterisks:** p < 0.05 throughout.
- **Bootstrap:** 2,000 resamples. R² and ΔR² use paired resamples within each age × sex
  stratum. The seed is set in `R/setup.R`.

---

## Configuration

Everything is set in `R/setup.R` (`paths`, `opts`). Paths can be overridden with environment
variables, for example in a `.Renviron` file in the repository root (git-ignored):

| Variable | Default | Purpose |
|---|---|---|
| `BMIKIDS_DATA` | `data/` | Where NHANES files and the CDC table are read from (and downloaded to) |
| `BMIKIDS_RESULTS` | `results/` | Where figures, tables, source data and stats are written |
| `BMIKIDS_DERIVED` | `<BMIKIDS_DATA>/derived/` | Where the analysis datasets and intermediate `.rds` results are written |
| `BMIKIDS_USE_2017_2018` | `FALSE` | Use the 2017–2018 cycle (with DXA) instead of 2017–March 2020 |
| `BMIKIDS_V1_WAIST_EXCLUSION` | `FALSE` | **Validation only:** re-apply the original exclusion of participants without a waist measurement |
| `RSTUDIO_PANDOC` | — | Folder that contains pandoc, if R cannot find it |

Relative paths are taken relative to the repository root.

---

## Source data and individual-level data

`results/source_data/` holds one CSV per figure plus `SourceData_AllFigures.xlsx`. Its README
sheet describes every column. The source data contain **no individual-level rows**. The two
panels that show individuals (Figure 1 and Figure 10A) are exported as counts in fine
grid cells. To regenerate the individual view, run `00_nhanes_fetch_data.R` and
`01_data_wrangling.R`, then the figure script; the individual points are read from
`data/derived/adults.rds` / `kids.rds`, rebuilt from the public NHANES files.

---

## Reproducing the manuscript exactly vs the corrected analysis

This repository regenerates every number in the manuscript. A few analysis details were
corrected during the rebuild. The most important is that participants without a waist
measurement are no longer excluded (1,332 people). As a result, reproduced values can differ
slightly from an earlier draft.
`results/stats/in_text_stats.csv` lists every quoted number with its reproduced value, and
`DISCREPANCIES.md` explains each difference.

To check the code against the original rules, run the pipeline with
`BMIKIDS_V1_WAIST_EXCLUSION=TRUE` and separate `BMIKIDS_RESULTS` and `BMIKIDS_DERIVED` folders. This validation mode
reproduced 70 of the 80 numbers quoted in the earlier draft (draft3) exactly. `stats/` now
holds the numbers as quoted in the current draft, which uses the corrected analysis.

---

## Repository layout

```
00_nhanes_fetch_data.R  01_data_wrangling.R  02_adults.Rmd  03_kids.Rmd  04_kids_and_adults.Rmd
run_all.R  renv.lock  bmikids.Rproj  LICENSE
figures/    one script per figure + make_source_data_workbook.R
tables/     tab02_beta_by_age.R
R/          setup.R (loaded by every script), check_in_text_stats.R, check_sample_sizes.R
stats/      manuscript_values.csv, manuscript_table2.csv (numbers as quoted in the manuscript)
reports/    knitted HTML of 02–04
data/       (not committed) downloaded NHANES and CDC files; derived/ holds the analysis datasets
results/    (not committed) everything the pipeline produces
```

## Troubleshooting

- **CSV files** are written as UTF-8 with a byte-order mark (`readr::write_excel_csv()`), so Excel shows symbols such as β, ², – and − correctly. R and other tools read them as normal.

- **Empty (0-byte) figure files, or "cannot open the connection"**: this can happen when `results/`
  is inside a cloud-synced folder (OneDrive, Dropbox) or a figure is open in a PDF viewer while
  it is rewritten. Close the viewer and re-run; `save_figure()` stops with an error rather than
  writing an empty file. Pointing `BMIKIDS_RESULTS` at a local folder avoids the problem.
- **"pandoc version 2.8 or higher is required"**: set `RSTUDIO_PANDOC` (see *Configuration*), or
  knit from RStudio.

## Licence

MIT; see `LICENSE`.
