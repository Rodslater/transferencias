library(dplyr)
library(tidyr)
library(stringr)
library(data.table)
library(downloader)
library(lubridate)
library(httr)
library(doParallel)

num_cores <- parallel::detectCores()
registerDoParallel(cores = num_cores)

data_atual <- as.Date(Sys.Date())

# Crie um vetor vazio para armazenar os meses e anos
datas <- character(0)

# Loop para criar as datas no formato desejado, até o mês atual
ano_atual <- as.integer(format(data_atual, "%Y"))
mes_atual <- as.integer(format(data_atual, "%m"))

for (ano in 2019:ano_atual) {
  max_mes <- ifelse(ano == ano_atual, mes_atual, 12)  # Limitar ao mês atual
  for (mes in 1:max_mes) {
    date <- sprintf("%d%02d", ano, mes) # Formate a data no estilo "YYYYMM"
    datas <- c(datas, date)
    datas <- as.numeric(datas)
  }
}



# Função para baixar e processar um arquivo
download_and_process <- function(data) {
  url <- paste0('https://portaldatransparencia.gov.br/download-de-dados/transferencias/', data, '.zip')
  arquivo <- sprintf("dataset_%s.zip", data)
  
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

# Número de processos paralelos a serem executados
num_cores <- parallel::detectCores()

# Executa o download e processamento em paralelo
results <- foreach(data = datas, .packages = c("httr", "lubridate"), .combine = c) %dopar% {
  download_and_process(data)
}

# Verifica se algum arquivo retornou NULL (não encontrado ou erro)
null_results <- which(sapply(results, is.null))
if (length(null_results) > 0) {
  message("Alguns arquivos não foram encontrados ou ocorreram erros.")
}



transferencias <- list.files(pattern = "\\.csv$", full.names = TRUE) 

colunas_transferencias <- c('ANO / MÊS','UF','NOME MUNICÍPIO','LINGUAGEM CIDADÃ','VALOR TRANSFERIDO')

transferencias <- rbindlist(lapply(transferencias, function(file) {
  fread(file, select = colunas_transferencias, encoding = "Latin-1", colClasses = "character")
}))


transferencias <- transferencias |> 
  filter(UF == 'SE') |> 
  mutate(`ANO / MÊS` = ymd(paste0(`ANO / MÊS`, "01"))) |> 
  mutate(`VALOR TRANSFERIDO` = as.numeric(str_replace(`VALOR TRANSFERIDO`, ",", ".")))

FPM <- transferencias |> 
  filter(`LINGUAGEM CIDADÃ` == 'FPM - CF art. 159')
  
  
arquivos_csv <- dir(pattern = ".csv")
file.remove(arquivos_csv)  


saveRDS(transferencias, 'data/transferencias.rds')
saveRDS(FPM, 'data/FPM.rds')
