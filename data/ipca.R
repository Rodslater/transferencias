library(sidrar)

ipca <- get_sidra(x = 1737,
                  variable = 2266,
                  period = 'all')

saveRDS(ipca, 'data/ipca.rds')
