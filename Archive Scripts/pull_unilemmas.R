# pull_unilemmas.R
# Pulls Polish and British English CDI items with unilemmas from Wordbank,
# joins them on uni_lemma, and writes a CSV for use in Python.
#
# Install dependencies (run once):
#   install.packages("remotes")
#   `remotes::install_github("langcog/wordbankr")`1
#   install.packages(c("wordbankr", "dplyr", "readr"))
#
# Run:
#   Rscript pull_unilemmas.R
#   (or source it inside RStudio)
#
# Output:
#   unilemma_pairs.csv  — matched Polish/English pairs sharing a unilemma

library(wordbankr)
library(dplyr)
library(readr)

cat("Fetching Polish WS items from Wordbank...\n")
no_items <- get_item_data("Polish", "WS") |>
  filter(item_kind == "word", !is.na(uni_lemma)) |>
  select(
    polish_word     = item_definition,
    polish_category = category,
    polish_gloss    = english_gloss,
    uni_lemma
  )

cat(sprintf("  Polish items with unilemma: %d\n", nrow(no_items)))

cat("Fetching British English WS items from Wordbank...\n")
en_items <- get_item_data("English (British)", "Oxford CDI") |>
  filter(item_kind == "word", !is.na(uni_lemma)) |>
  select(
    english_word     = item_definition,
    english_category = category,
    uni_lemma
  )

cat(sprintf("  British English items with unilemma: %d\n", nrow(en_items)))

# Join on uni_lemma — these are human-verified translation equivalents
cat("Joining on uni_lemma...\n")
pairs <- inner_join(no_items, en_items, by = "uni_lemma", relationship = "many-to-many") |>
  select(
    uni_lemma,
    polish_word,
    polish_category,
    polish_gloss,
    english_word,
    english_category
  ) |>
  arrange(uni_lemma, polish_word, english_word)

cat(sprintf("  Matched pairs: %d\n", nrow(pairs)))
cat(sprintf("  Unique unilemmas matched: %d\n", n_distinct(pairs$uni_lemma)))

# Also write out unmatched Polish words (no English equivalent in Wordbank)
no_unmatched <- no_items |>
  filter(!uni_lemma %in% pairs$uni_lemma) |>
  select(polish_word, polish_category, polish_gloss, uni_lemma)

cat(sprintf("  Polish words with NO English match: %d\n", nrow(no_unmatched)))

# Write outputs
write_csv(pairs,        "en_pl_unilemma_pairs.csv")
write_csv(no_unmatched, "pl_unmatched.csv")

cat("\nDone!\n")
cat("  en_pl_unilemma_pairs.csv      — use this as input to the Python pipeline\n")
cat("  pl_unmatched.csv — Polish words with no Wordbank English equivalent\n")
d <- get_instruments()
# Quick preview
cat("\nFirst 20 matched pairs:\n")
print(head(pairs |> select(uni_lemma, polish_word, english_word, polish_category), 20))