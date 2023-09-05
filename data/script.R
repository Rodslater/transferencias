library(dplyr)
library(tidyr)
library(rvest)
library(data.table)
library(lubridate)
library(httr)
library(doParallel)

site <- read_html("https://www.tesourotransparente.gov.br/ckan/dataset/transferencias-constitucionais-para-municipios")

links <- site %>%
  html_nodes("a") %>%
  html_attr("href")

# Filtrar os links 
links <- grep("TransferenciaMensalMunicipios2019|TransferenciaMensalMunicipios202.*\\.csv$", links, value = TRUE)
# Baixar e salvar os arquivos com seus nomes originais


###Teste em paralelo


# Função para baixar e processar um arquivo
download_and_process <- function(url) {
  arquivo <- sub(".*/", "", url)  # Obtém o nome do arquivo do URL
  
  # Verifica se o arquivo existe antes de fazer o download
  response <- tryCatch(
    {
      HEAD(url)
    },
    error = function(e) {
      return(NULL)
    }
  )
  
  # Se a resposta é NULL, o arquivo não existe, então retorna
  if (is.null(response)) {
    message(paste("Arquivo não encontrado:", arquivo))
    return(NULL)
  }
  
  tryCatch(
    {
      download.file(url, destfile = arquivo, mode = "wb")
      unzip(arquivo)
      file.remove(arquivo)
      arquivos_csv <- list.files(pattern = "\\.csv$", full.names = TRUE)
      padroes_mantidos <- c("\\.csv$")
      arquivos_remover <- arquivos_csv[!grepl(paste(padroes_mantidos, collapse = "|"), arquivos_csv)]
      file.remove(arquivos_remover)
    },
    error = function(e) {
      message(paste("Erro ao baixar o arquivo:", arquivo))
      return(NULL)
    }
  )
}

# Vetor de URLs a serem processadas
urls <- links

# Número de processos paralelos a serem executados
num_cores <- parallel::detectCores()
registerDoParallel(cores = num_cores)

# Executa o download e processamento em paralelo
results <- foreach(url = urls, .packages = c("httr", "lubridate"), .combine = c) %dopar% {
  download_and_process(url)
}





transferencias_tesouro <- list.files(pattern = "\\.csv$", full.names = TRUE) 

transferencias_tesouro <- rbindlist(lapply(transferencias_tesouro, function(file) {
  fread(file, encoding = "Latin-1")
}), fill = TRUE)
#fill = TRUE, para quando há divergência no número de colunas


transferencias_tesouro <- transferencias_tesouro |> 
  filter(UF == 'SE') |> 
  mutate_at(vars(c("1º Decêndio", "2º Decêndio", "3º Decêndio")), as.numeric) |> 
  unite(ANO, ANO, Ano, sep = "", na.rm = TRUE) |> 
  relocate(.after = last_col(), contains("Decêndio")) |> 
  mutate(Mês = ymd(paste0(ANO, "-", Mês, "-01")),
         `Valor total` = `1º Decêndio`+`2º Decêndio`+`3º Decêndio`,
         Município = case_when(Município  == 'Amparo de São Francisco' ~ 'Amparo do São Francisco',
                               Município  == 'Gracho Cardoso' ~ 'Graccho Cardoso',
                               Município  == 'Itaporanga DAjuda' ~ "Itaporanga D'Ajuda",
                               TRUE ~ Município)) |> 
  select(-c(V10, UF, ANO)) |> 
  relocate(Mês, .before = everything())


arquivos_csv <- dir(pattern = ".csv")
file.remove(arquivos_csv)  

FPM_tesouro <- transferencias_tesouro |> 
  filter(Transferência == 'FPM') 

saveRDS(transferencias_tesouro, 'data/transferencias_tesouro.rds')
saveRDS(FPM_tesouro, 'data/FPM_tesouro.rds')

write.csv(transferencias_tesouro, "data/transferencias_tesouro.csv", row.names = FALSE)
write.csv(FPM_tesouro, "data/FPM_tesouro.csv", row.names = FALSE)
