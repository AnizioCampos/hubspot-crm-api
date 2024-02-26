# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import gspread

token_account_1 = 'toke_account_1'
token_account_2 = 'token_account_2'
token_account_3 = 'token_account_3'

# Função responsável por colocar dado no Google Sheets usando a API.

def setDataGoogleSheets(sheet_name, df):
  #código da planilha que o dado será adicionado.
  CODE = 'CODE'
  credenciais ={
                "type": "service_account",
                "project_id": "python-sheets-hubspot",
                "private_key_id": "private_key_id",
                "private_key": "private-key\n",
                "client_email": "python-connect-hubspot-sheets@python-sheets-hubspot.iam.gserviceaccount.com",
                "client_id": "client_id",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/python-connect-hubspot-sheets%40python-sheets-hubspot.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com"
              }
  #client para acessar o sheets com a nossa key de acesso.
  gc = gspread.service_account_from_dict(credenciais) #a key está sendo acessada de um diretório local.
  #sheet que queremos adicionar os dados dados.
  sh = gc.open_by_key(CODE)
  #escolhendo uma sheet específica para adicionar nossos dados
  ws = sh.worksheet(sheet_name)
  #adicionando os dados do formato Data Frame do pandas para dentro da sheet Faturamento.
  ws.update([df.columns.values.tolist()] + df.values.tolist())

"""# DEALS"""
# Função responsável por coletar dados de negócios do endpoint no Hubspot
def func_deals(token, company):
  #Variavel responsavel por nos dar a proxima pagina.
  var_after = "0"
  #Lista onde todos os payloads serão guardados.
  all_deals = []
  while True:
    try:
      # Link do endpoint.
      url2 = "https://api.hubapi.com/crm/v3/objects/deal"
      payload = {}
      # Token para autenticação.
      headers = {
        'Authorization': 'Bearer ' + token
      }
      # parametros para definição de "limite", "properties", "after".
      querystring = {"limit":"100",
                    "archived":"false",
                    "properties":['DealId', 'closedate', 'dealstage', 'dealtype', 'hs_arr', 'hs_mrr', 'hs_tcv','pipeline',
                                  'amount', 'hs_acv', 'dealname', 'description', 'amount_in_home_currency',
                                  'hs_exchange_rate', 'days_to_close', 'perfil_da_empresa__numero_de_funcionarios_','motivo_de_perda',
                                  'origem_de_negocio'],
                    "after":var_after}
      response = requests.request("GET", url2, headers=headers, data=payload, params=querystring)
      response_deals = json.loads(response.text)
      #Pegando os deals vindos do request atual e adicionando na lista geral.
      for deal in response_deals['results']:
        all_deals.append(deal)
      #Variavel responsavel por pegar as proximas paginas.
      var_after = response_deals['paging']['next']['after']
    except KeyError as e:
      # Verifica se a paginação acabar quebra o loop.
      print('End of pagination...')
      break
  #################################################
  ############## DATA WRANGGLING ##############
  #################################################
  dataframe_deals = pd.DataFrame(columns=['closedate', 'createdate', 'dealstage', 'dealtype', 'hs_arr', 'hs_lastmodifieddate', 'hs_mrr',
                                                'deal_id', 'hs_tcv', 'pipeline', 'amount', 'hs_acv', 'dealname', 'description',
                                              'origem_de_negocio', 'amount_in_home_currency', 'hs_exchange_rate',
                                              'empresa_proprietaria', 'days_to_close',
                                          'perfil_da_empresa__numero_de_funcionarios_', 'motivo_de_perda'])

  for i in range(len(all_deals)):
      currentItem_closedate = all_deals[i]['properties']['closedate']
      currentItem_createdate = all_deals[i]['properties']['createdate']
      currentItem_dealstage = all_deals[i]['properties']['dealstage']
      currentItem_dealtype = all_deals[i]['properties']['dealtype']
      currentItem_hs_arr = all_deals[i]['properties']['hs_arr']
      currentItem_hs_lastmodifieddate = all_deals[i]['properties']['hs_lastmodifieddate']
      currentItem_hs_mrr = all_deals[i]['properties']['hs_mrr']
      currentItem_deal_id = all_deals[i]['properties']['hs_object_id']
      currentItem_hs_tcv = all_deals[i]['properties']['hs_tcv']
      currentItem_pipeline = all_deals[i]['properties']['pipeline']
      currentItem_amount = all_deals[i]['properties']['amount']
      currentItem_hs_acv = all_deals[i]['properties']['hs_acv']
      currentItem_dealname = all_deals[i]['properties']['dealname']
      currentItem_description = all_deals[i]['properties']['description']
      currentItem_origem_de_negocio = all_deals[i]['properties'].get('origem_de_negocio', None)
      currentItem_amount_in_home_currency = all_deals[i]['properties']['amount_in_home_currency']
      currentItem_hs_exchange_rate = all_deals[i]['properties']['hs_exchange_rate']
      current_empresa_proprietaria = all_deals[i]['properties'].get('empresa_proprietaria', empresa_proprietaria)
      currentItem_days_to_close = all_deals[i]['properties']['days_to_close']
      currentItem_perfil_da_empresa__numero_de_funcionarios_ = all_deals[i]['properties'].get('perfil_da_empresa__numero_de_funcionarios_', None)
      currentItem_motivo_de_perda = all_deals[i]['properties'].get('motivo_de_perda', None)

      dataframe_deals.loc[i] = [currentItem_closedate, currentItem_createdate, currentItem_dealstage, currentItem_dealtype, currentItem_hs_arr,
                                    currentItem_hs_lastmodifieddate, currentItem_hs_mrr, currentItem_deal_id, currentItem_hs_tcv,
                                    currentItem_pipeline, currentItem_amount, currentItem_hs_acv, currentItem_dealname, currentItem_description,
                                    currentItem_origem_de_negocio, currentItem_amount_in_home_currency, currentItem_hs_exchange_rate,
                                    current_empresa_proprietaria, currentItem_days_to_close,
                                currentItem_perfil_da_empresa__numero_de_funcionarios_, currentItem_motivo_de_perda]

  return dataframe_deals

"""# Account_1 Deals"""

df_deals_account_1 = func_deals(token_account_1, 'account_1')

"""# Account_2 Deals"""

df_deals_account_2 = func_deals(token_account_2, 'account_2')

"""# Account_3 Deals"""

df_deals_account_3 = func_deals(token_account_3, 'account_3')

# Concatenando os dataframes de Deals da account_1 com account_2 num único dataframe chamado Total
df_deals_total = pd.concat([df_deals_account_1, df_deals_account_2]).copy()
# Concatenando o dataframe de Deals da account_2 com o Total criado anteriormente.
df_deals_total = pd.concat([df_deals_total, df_deals_account_3]).copy()

# Adicionando os dados de todos os line items dentro da planilha no google sheets
setDataGoogleSheets('all_deals', df_deals_total)

"""# LINE ITENS

## Transformando o script que pega dado do endpoint de items de linha em uma função para melhor organização do código
"""

def func_line_items(token):

  #Variavel responsavel por nos dar a proxima pagina.
  var_after = "0"

  #Lista onde todos os payloads serão guardados.
  all_line_items = []

  while True:
      try:
          # Link do endpoint.
          url = "https://api.hubapi.com/crm/v3/objects/line_items?associations=deals"
          payload = {}
          # Token para autenticação.
          headers = {
              'Authorization': 'Bearer ' + token
          }
          querystring = {"limit":"100",
                      "archived":"false",
                      "properties":['amount', 'hs_acv', 'hs_line_item_currency_code', 'hs_product_id', 'hs_sku', 'hs_tcv',
                      'name', 'price', 'quantity', 'renovacao_automatica', 'categoria_produto_servico_do_item_de_linha',
                      ],
                      "after":var_after}
          # parametros para definição de "limite", "properties", "after".
          response = requests.request("GET", url, headers=headers, data=payload, params=querystring)
          response_line_items = json.loads(response.text)
          #Pegando os deals vindos do request atual e adicionando na lista geral.
          for line_item in response_line_items['results']:
              all_line_items.append(line_item)
          #Variavel responsavel por pegar as proximas paginas.
          var_after = response_line_items['paging']['next']['after']
      except KeyError as e:
          print('Fim da paginacao..')
          break
  #################################################
  ############## DATA WRANGGLING ##############
  #################################################

  dataframe_line_items = pd.DataFrame(columns=['hs_object_id', 'amount', 'createdate', 'hs_acv',
                                                      'hs_lastmodifieddate', 'hs_line_item_currency_code',
                                                      'hs_product_id', 'hs_sku', 'hs_tcv', 'name', 'price',
                                                      'quantity', 'categoria_produto_servico_do_item_de_linha'])

  for i in range(len(all_line_items)):
      currentItem_hs_object_id = all_line_items[i]['properties']['hs_object_id']
      currentItem_amount = all_line_items[i]['properties']['amount']
      currentItem_createdate = all_line_items[i]['properties']['createdate']
      currentItem_hs_acv = all_line_items[i]['properties']['hs_acv']
      currentItem_hs_lastmodifieddate = all_line_items[i]['properties']['hs_lastmodifieddate']
      currentItem_hs_line_item_currency_code = all_line_items[i]['properties']['hs_line_item_currency_code']
      currentItem_hs_product_id = all_line_items[i]['properties']['hs_product_id']
      currentItem_hs_sku = all_line_items[i]['properties']['hs_sku']
      currentItem_hs_tcv = all_line_items[i]['properties']['hs_tcv']
      currentItem_name = all_line_items[i]['properties']['name']
      currentItem_price = all_line_items[i]['properties']['price']
      currentItem_quantity = all_line_items[i]['properties']['quantity']
      currentItem_categoria_produto_servico_do_item_de_linha = all_line_items[i]['properties'].get('categoria_produto_servico_do_item_de_linha', None)

      dataframe_line_items.loc[i] = [currentItem_hs_object_id, currentItem_amount, currentItem_createdate,
                                            currentItem_hs_acv, currentItem_hs_lastmodifieddate, currentItem_hs_line_item_currency_code,
                                            currentItem_hs_product_id, currentItem_hs_sku, currentItem_hs_tcv,
                                            currentItem_name, currentItem_price, currentItem_quantity, currentItem_categoria_produto_servico_do_item_de_linha]
  return dataframe_line_items

"""## Line Itens - account_1"""

# Pegando dados da API de Items de Linha do Hubspot no endpoint da account_1
df_lineItems_account_1 = func_line_items(token_account_1)

"""## Line Itens - account_2"""

# Pegando dados da API de Items de Linha do Hubspot no endpoint da account_2
df_lineItems_account_2 = func_line_items(token_account_2)

"""## Line Itens - account_3"""

# Pegando dados da API de Items de Linha do Hubspot no endpoint da account_3
df_lineItems_account_3 = func_line_items(token_account_3)

# Concatenando os dataframes de Line Items da account_1 com account_2 num único dataframe chamado Total
df_lineItens_total = pd.concat([df_lineItems_account_1, df_lineItems_account_2]).copy()
# Concatenando o dataframe de Line Items da account_2 com o Total criado anteriormente.
df_lineItens_total = pd.concat([df_lineItens_total, df_lineItems_account_3]).copy()

# Adicionando os dados de todos os line items dentro da planilha no google sheets
setDataGoogleSheets('all_line_itens', df_lineItens_total)

"""# (DEAL <-> LINE ITEMS) ASSOCIATIONS

## Transformando o processo de retirada de chaves Deal-LineItem em funções para melhor organização do código
"""

def deal_line_item_associations(token):
  #Variavel responsavel por nos dar a proxima pagina.
  var_after = "0"
  #Lista onde todos os payloads serão guardados.
  all_deals = []
  while True:
    try:
      # Link do endpoint.
      url2 = "https://api.hubapi.com/crm/v3/objects/deal?associations=line_item"
      payload = {}

      # Token para autenticação.
      headers = {
        'Authorization': 'Bearer ' + token
      }
      # parametros para definição de "limite", "properties", "after".
      querystring = {"limit":"100",
                    "archived":"false",
                    "after":var_after}
      response = requests.request("GET", url2, headers=headers, data=payload, params=querystring)
      response_deals_line_items = json.loads(response.text)
      #Pegando os deals vindos do request atual e adicionando na lista geral.
      for deal in response_deals_line_items['results']:
        all_deals.append(deal)
      #Variavel responsavel por pegar as proximas paginas.
      var_after = response_deals_line_items['paging']['next']['after']
    except KeyError as e:
      # Verifica se a paginação acabar quebra o loop.
      break
  ############### TRARTAMENTO DE DADOS ###############
  dataframe_deals_line_items_association = pd.DataFrame(columns=['deal_id', 'line_item_id'])

  for i_deal in range(len(all_deals)):
      # Access 'associations' safely, return an empty dict if not found
      associations = all_deals[i_deal].get('associations', {})
      line_items = associations.get('line items', {})

      results = line_items.get('results', {})
      deal_id = all_deals[i_deal]['id']
      for j_line_item in range(len(results)):
        line_item_id = results[j_line_item]
        currentDealId = all_deals[i_deal]['id']
        currentLineItem = results[j_line_item]['id']
        new_row_df = pd.DataFrame({'deal_id': [str(currentDealId)], 'line_item_id': [str(currentLineItem)]})
        dataframe_deals_line_items_association = pd.concat([dataframe_deals_line_items_association, new_row_df], ignore_index=True)
  #print(dataframe_deals_line_items_association.shape)
  return dataframe_deals_line_items_association

"""### DEALS -> LINE ITENS ASSOCIATIONS (account_1)"""

# Pegando os dados da API de deals-lineItens-associations do Hubspot usando o token da account_1 e tratando
df_deals_line_items_associations_account_1 = deal_line_item_associations(token_account_1)

"""### DEALS -> LINE ITENS ASSOCIATIONS (account_2)"""

# Pegando os dados da API de deals-lineItens-associations do Hubspot usando o token da account_2 e tratando
df_deals_line_items_associations_account_2 = deal_line_item_associations(token_account_2)

"""### DEALS -> LINE ITENS ASSOCIATIONS (account_3)"""

# Pegando os dados da API de deals-lineItens-associations do Hubspot usando o token da account_3 e tratando
df_deals_line_items_associations_account_3 = deal_line_item_associations(token_account_3)

# Concatenando os dataframes de deals-lineItems-associations da account_1 com account_2 num único dataframe chamado Total
df_deals_line_items_associations_total = pd.concat([df_deals_line_items_associations_account_1, df_deals_line_items_associations_account_2])
# Concatenando o dataframe de deals-lineItems-associations da account_2 com o Total criado anteriormente.
df_deals_line_items_associations_total = pd.concat([df_deals_line_items_associations_total, df_deals_line_items_associations_account_3])

"""### SET THE DATA INSIDE THE GOOGLE SHEETS"""

# Adicionando os dados dentro da planilha no google sheets
setDataGoogleSheets('deals_line_items_associations', df_deals_line_items_associations_total)

print(df_deals_line_items_associations_total.shape)