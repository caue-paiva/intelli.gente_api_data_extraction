from AbstractApiInterface import AbstractApiInterface
from DataPoint import DataPoint , DataPointTypes
import requests
import pandas as pd

"""
TODO

1) criar uma lógica nessa classe (e talvez na superclasse de forma mais geneŕica) para lidar com o ID dos agregados na api de agregados do IBGE

"""


class IbgeAgregatesApi(AbstractApiInterface):

   DB_TO_API_DATA_MAP = {
      "PIB TOTAL" :  37, 
      "PIB AGROPECUARIA": 513,
      "PIB INDUSTRIA": 517,
      "PIB SERVICOS" : 6575,
      "PIB ADMINISTRACAO PUBLICA" : 525
   }

   api_name:str
   goverment_agency:str

   def __init__(self, api_name: str, goverment_agency: str) -> None:
      self.api_name = api_name
      self.goverment_agency = goverment_agency

   def __response_to_data_points(self,api_response:list[dict])->list[DataPoint]:
      return_data_points:list[DataPoint] = []
      
      for data_point in api_response: #loop por cada dado bruto/variável na resposta
         data_name:str  | None = data_point.get("variavel") #nome do dado
         data_unit: str | None = data_point.get("unidade")  #unidade que o dado está
         results: list[dict]| None = data_point.get("resultados", [])[0].get("series", []) #valores do resultado

         if any(x is None for x in [data_name,data_unit,results]): #verifica se foi possível acessar todos os campos
               raise IOError(f"Não foi possível obter uma variável da lista { [data_name,data_unit,results]}")
         
         for city in results: #loop pelos municípios
            city_id:int | None = int(city.get("localidade", {}).get("id"))
            city_name: str | None = city.get("localidade", {}).get("nome")
            time_series: dict | None = city.get("serie") #dicionário com a série histórica dos dados

            if any(x is None for x in [city_id,city_name,time_series]):  #verifica se foi possível acessar todos os campos
               raise IOError(f"Não foi possível obter uma variável da lista {[city_id,city_name,time_series]}")

            for year, value in time_series.items(): #loop pelo dicionario com o ano como chave e o valor do dado como value
               new_data_point:DataPoint = DataPoint(city_id, year, data_name, DataPointTypes.FLOAT) #cria um novo ponto de dado, mas sem o numero de multiplicação
               #nem o valor
               
               inference_result:bool = new_data_point.infer_dtype_and_multiply_amnt(data_unit) #tentar inferir o numero pra multiplicar o valor da unidade obtida anteriormente
               if not inference_result:
                  raise RuntimeError("Não foi possível inferir o tipo de dado e qntd de multiplicar do dado")
               
               new_data_point.value = new_data_point.multiply_value(value) #campo de valor do data_point recebe o valor lida da API e multiplicado
               return_data_points.append(new_data_point) #adiciona esse data point na lista
                  
      return return_data_points
   
   def extract_data_points(self, cities: list[int], data_point_names: list[str] = [] , time_series_len: int = 0) -> list[DataPoint]:
      if time_series_len > self.MAX_TIME_SERIES_LEN:
         raise IOError(f"tamanho da série temporal em anos excede o limite de {self.MAX_TIME_SERIES_LEN} anos")
      
      if time_series_len == 0:
         time_series_len = self.MAX_TIME_SERIES_LEN

      if not cities:
         #lógica para incluir todas as cidades
         pass

      api_data_variables: list[int | str] = []
      if not data_point_names: #vamos usar todos os dados no mapeamento criado para essa subclasse
         api_data_variables = list(self.DB_TO_API_DATA_MAP.values())
      else: #vamos usar apenas os dados da lista provida
         for point in data_point_names:
            data_api_code: int|str|None  = self.DB_TO_API_DATA_MAP.get(point)
            
            if data_api_code is None:
               raise IOError(f"O dado {point} do argumento da lista de nomes dos dados não pertence ao mapeamento DB_TO_API da classe")
            
            api_data_variables.append(data_api_code) #add o código da API referente ao dado na lista


      base_url = "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variaveis}"

      str_data_variables:str = '|'.join(map(str, api_data_variables))
      params = {
         'localidades': f'N6{cities}'
      }
      url:str = base_url.format(agregado=5938, periodos= (-time_series_len), variaveis=str_data_variables)
      response = requests.get(url, params=params, verify=False)

      if response.status_code == 200: #request teve sucesso
         response_data:list[dict] = response.json()
         return self.__response_to_data_points(response_data)
      else:
         raise RuntimeError("Falha na Request para a API")





if __name__ == "__main__":
   api1 = IbgeAgregatesApi("api agregados", "ibge")

   d_points:list[DataPoint] = api1.extract_data_points([1100072,1100023],time_series_len=7)
   print(d_points[0].data_type)
   df : pd.DataFrame = api1.data_points_to_df(d_points)
   print(df.head(5))
   print(df.shape)
   print(df["ano"].value_counts())
