pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue)

ctd <- biobricks::bbload("ctdbase")

# chem-gene-ixn
cgi <- ctd$CTD_chem_gene_ixns
gcnt <- cgi |> count(GeneID,sort=T) |> collect()

cgi |> filter(GeneID==9241) |> count(InteractionActions) |>
collect()

# chem-disease-ixn
cdi <- ctd$CTD_chem_disease_ixns 