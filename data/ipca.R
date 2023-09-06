library(sidrar)

ipca <- get_sidra(api = "/t/1737/n1/all/v/2266/p/all/d/v2266%2013")

saveRDS(ipca, 'data/ipca.rds')
