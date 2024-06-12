from ApiHelperClasses.AbstractApiInterface import AbstractApiInterface
from ApiHelperClasses.DataLine  import DataLine, DataLineTypes
from ApiHelperClasses.DataCollections import RawDataCollection ,ProcessedDataCollection
import requests,json , os, urllib3 , time

#ao chamar a API do IBGE a lib de requests fica reclamando que a conexão é insegura, essa linha desabilita isso
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
TODO

1) Fazer alguma coisa para caso a lista de cidades não seja passada, pegar todas as cidades do país

2) Extrair dados do Percentual de domicílios com população vivendo em aglomerados subnormais (dado é o indicador)

"""

def __print_processed_data(data_list:list[ProcessedDataCollection]):
   for collection in data_list:
      print(collection.data_name)
      print(collection.category)
      print(collection.df.head(5))
      print(collection.time_series_years)



class IbgeAgregatesApi(AbstractApiInterface):

   IBGE_NAN_CODES:dict[str,dict] = { #Códigos que o IBGE adota para valores fora do normal na sua API
      "-": {"val": 0, "type": DataLineTypes.INT}, #Dado numérico igual a zero não resultante de arredondamento
      "..": {"val": None,"type":DataLineTypes.NULL}, #Não se aplica dado numérico
      "...": {"val": None,"type":DataLineTypes.NULL},  #Dado numérico não disponível
      "X":   {"val": None,"type":DataLineTypes.NULL} #Dado numérico omitido a fim de evitar a individualização da informação
   }

   api_name:str
   goverment_agency:str
   _data_map: dict[str, dict[str,dict]]

   def __init__(self, api_name: str, goverment_agency: str, api_referen_json_path:str) -> None:
      self.api_name = api_name
      self.goverment_agency = goverment_agency

      self._db_to_api_data_map(api_referen_json_path) #mapea o JSON de referencia da API num dict que vai ser usado pela classe
      
   def _db_to_api_data_map(self, api_reference_json_path:str)->None:
      """
      Carrega o JSON que mapea cada ponto de dado do BD em argumentos para a chamada da API do IBGE em um dicionário que será usado por essa classe
      """
      try:
         with open(os.path.join(api_reference_json_path), "r") as f:
            loaded_data = json.load(f)
            if not isinstance(loaded_data,dict):
               raise IOError("objeto json não está na forma de um dicionário do python")
            self._data_map: dict[str, dict[str,dict]] = loaded_data
      except Exception as e:
           raise RuntimeError("Não foi possível carregar o JSON que mapea os dados do DB para a API")

   def __find_data_name_category_by_id(self,variable_id:int ,classification:str = "")-> tuple[str] | None:
      """
      
      return: 
            (nome_dado , categoria_dado)
      
      """
      
      for category in self._data_map:
         for variable, api_params in self._data_map[category].items():
            
            if not classification:
               if api_params.get("variavel") == variable_id:
                  return (variable,category) 
            else:
               if api_params.get("variavel") == variable_id and api_params.get("classificacao") == classification:
                  return (variable,category) 
      return None

   def __process_single_api_result(self,list_of_cities:list[dict],data_name:str,data_unit:str)->RawDataCollection:
      """
      
      return:
         RawDataCollection (objeto) sem a classe e o nome do dado, apenas a série historica e a lista de dados
   
      """
      
      return_data_points:list[DataLine] = []
      series_years:list[int] = []#variavel para guardar os anos da série histórica dos dados

      for city in list_of_cities: #loop pelos municípios
         city_id:int | None = int(city.get("localidade", {}).get("id"))
         city_name: str | None = city.get("localidade", {}).get("nome")
         time_series: dict | None = city.get("serie") #dicionário com a série histórica dos dados

         if any(x is None for x in [city_id,city_name,time_series]):  #verifica se foi possível acessar todos os campos
                  raise IOError(f"Não foi possível obter uma variável da lista {[city_id,city_name,time_series]}")
         
         new_series_years:list[int] = list(map(int,time_series.keys())) #transforma as keys do dict de series em uma lista de int
         if len(new_series_years) > len(series_years): #vamos pegar a série histórica maior entre os municípios, assim se um não tiver dados naquele ano o tamanho da série n diminiu
            series_years = new_series_years
        
         for year, value in time_series.items(): #loop pelo dicionario com o ano como chave e o valor do dado como value
            new_data_point:DataLine = DataLine(city_id, year, data_name,value) #cria um novo ponto de dado, mas sem o numero de multiplicação #nem o valor   
            if value in self.IBGE_NAN_CODES:  #o valor representa um código especial do IBGE para valores com problemas
               new_data_point.value = self.IBGE_NAN_CODES[value]["val"]
               new_data_point.data_type = self.IBGE_NAN_CODES[value]["type"]
            else: #o valor é normal, tenta inferir seu tipo
               inference_result:bool = new_data_point.infer_dtype_and_multiply_amnt(data_unit) #tentar inferir o numero pra multiplicar o valor da unidade obtida anteriormente
               if not inference_result:
                  print("Não foi possível inferir o tipo de dado e qntd de multiplicar do dado")

            return_data_points.append(new_data_point) #adiciona esse data point na lista

      return RawDataCollection("","",series_years, return_data_points)

   def __api_to_data_points(self,api_response:list[dict], classification:str="")->RawDataCollection:
      """
      
      Return:
          RawDataCollection (objeto) com todos os campos
      """

      if len(api_response) > 1:
         raise IOError("Função __api_to_data_points foi feita para processar chamadas de API com apenas uma variável por vez, a lista de resposta da API ultrapassou esse limite")
      
      variable:dict = api_response[0]
      
      return_data_points:list[DataLine] = []
      return_series_years:list[int] = []
               
      variable_id: int | None = int(variable.get("id")) #id da variável
      if variable_id is None:
            raise IOError("não foi possível acessar o id da variável")
         
      data_name, data_category  = self.__find_data_name_category_by_id(variable_id,classification)
      data_unit: str | None = variable.get("unidade")  #unidade que o dado está
         
      results_list:list[dict]| None = variable.get("resultados", [])
      if any(x is None for x in [variable_id,data_unit,results_list,data_name]): #verifica se foi possível acessar todos os campos
            raise IOError(f"Não foi possível obter uma variável da lista { [variable_id,data_unit,results_list,data_name]}")

      for result in results_list: #lista de resultados da variável, usado quando existe uma classificação do dado ex: queremos todas as casas com pavimentação parcial e pavimentação completa
         list_of_cities: list[dict]| None = result["series"] #valores do resultado
         single_api_result: RawDataCollection = self.__process_single_api_result(list_of_cities,data_name,data_unit)
            
         result_data_lines:list[DataLine] = single_api_result.data_lines #dados e anos da série histórica para um resultado da API
         result_series_years:list[int] = single_api_result.time_series_years

         if not return_data_points:
               return_data_points = result_data_lines
         else:
            add_data_point_values = lambda x, y: x.value + y.value
            summed_vals = list(map(add_data_point_values, return_data_points, result_data_lines))
            
            for index, val in enumerate(summed_vals):
                  return_data_points[index].value = val

         if len(result_series_years) > len(return_series_years): #caso a série histórica seja maior, ela se torna a nova série de retorno
            return_series_years = result_series_years 

      return RawDataCollection(
         data_category,
         data_name,
         result_series_years,
         return_data_points
      )
     
   def __make_api_call(self,time_series_len:int,cities:list[int],aggregate:int ,variable:str = "", classification:str = "")->RawDataCollection:
      """
      Return:
          RawDataCollection (objeto) com todos os campos
      """
      params:dict = {}
      if not classification:
         params = {'localidades': f'N6'}
      else:
         params = {"classificacao": classification ,'localidades': 'N6'}

      base_url:str = "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variaveis}"

      url:str = base_url.format(agregado=aggregate , periodos= (-time_series_len), variaveis=variable)
      response = requests.get(url, params=params, verify=False)
      if response.status_code == 200: #request teve sucesso
         response_data:list[dict] = response.json()
         return self.__api_to_data_points(response_data,classification) #adiciona os elementos retornados a lista final de pontos de dados
      else:
         raise RuntimeError("Falha na Request para a API")

   def extract_data_points(self, cities: list[int], time_series_len: int = 0) -> list[RawDataCollection]:
      """
      
      return:
         list[ {"nome_dado": "PIB" , "anos_serie": [2022,2012...] , "categoria": "economia", "lista_dados": list[DataLine] }, ...]
      """
      
      
      if time_series_len > self.MAX_TIME_SERIES_LEN:
         raise IOError(f"tamanho da série temporal em anos excede o limite de {self.MAX_TIME_SERIES_LEN} anos")
      
      if time_series_len == 0:
         time_series_len = self.MAX_TIME_SERIES_LEN

      if not cities:
         #lógica para incluir todas as cidades
         pass
      
      api_data_points: list[dict] = []  #lista com  dicionário, cada um representando um dado, contendo chaves para o nome do dado,sua categoria, anos de série históricas 
      #e uma lista de pontos de dados extraidos de todas as chamadas da API
      
      for category in self._data_map: #loop por todas as categorias de dados
         for data_point, api_call_params in self._data_map[category].items(): #loop por cada dado individual
            aggregate:int = api_call_params.get("agregado") #agregada o qual pertence o dado
            var: str = str(api_call_params.get("variavel")) #variavel do dado, transforma em string
            classification: str = api_call_params.get("classificacao","") #classificação da variável, caso exista
            
            call_result:RawDataCollection = self.__make_api_call(time_series_len,cities,aggregate,var,classification)
            print("chamada")
            time.sleep(2)
            api_data_points.append(call_result)

      return api_data_points

if __name__ == "__main__":
   json_path:str = os.path.join("AgregadoApiJsons","IbgeAgregatesApiDataMap.json")
   api1 = IbgeAgregatesApi("api agregados", "ibge",json_path)

   d_points:list[RawDataCollection] = api1.extract_data_points([1100072,1100023],time_series_len=7)
   
   data_list:list[ProcessedDataCollection] = api1.process_raw_data(d_points)
   __print_processed_data(data_list)

   df = data_list[0].df
   df =  df [df["ano"] == "2015" ]
   df.to_csv(os.path.join("dados_extraidos","base_agregados_todos_munic.csv"))